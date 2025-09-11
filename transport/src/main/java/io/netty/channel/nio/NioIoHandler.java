/*
 * Copyright 2024 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.channel.nio;

import io.netty.channel.ChannelException;
import io.netty.channel.DefaultSelectStrategyFactory;
import io.netty.channel.IoHandlerContext;
import io.netty.channel.IoHandle;
import io.netty.channel.IoHandler;
import io.netty.channel.IoHandlerFactory;
import io.netty.channel.IoOps;
import io.netty.channel.IoRegistration;
import io.netty.channel.SelectStrategy;
import io.netty.channel.SelectStrategyFactory;
import io.netty.util.IntSupplier;
import io.netty.util.concurrent.ThreadAwareExecutor;
import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.ReflectionUtil;
import io.netty.util.internal.StringUtil;
import io.netty.util.internal.SystemPropertyUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.Selector;
import java.nio.channels.SelectionKey;

import java.nio.channels.spi.SelectorProvider;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link IoHandler} implementation which register the {@link IoHandle}'s to a {@link Selector}.
 */
public final class NioIoHandler implements IoHandler {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(NioIoHandler.class);

    private static final int CLEANUP_INTERVAL = 256; // XXX Hard-coded value, but won't need customization.

    private static final boolean DISABLE_KEY_SET_OPTIMIZATION =
            SystemPropertyUtil.getBoolean("io.netty.noKeySetOptimization", false);

    private static final int MIN_PREMATURE_SELECTOR_RETURNS = 3;
    private static final int SELECTOR_AUTO_REBUILD_THRESHOLD;

    private final IntSupplier selectNowSupplier = new IntSupplier() {
        @Override
        public int get() throws Exception {
            return selectNow();
        }
    };

    // Workaround for JDK NIO bug.
    //
    // See:
    // - https://bugs.openjdk.java.net/browse/JDK-6427854 for first few dev (unreleased) builds of JDK 7
    // - https://bugs.openjdk.java.net/browse/JDK-6527572 for JDK prior to 5.0u15-rev and 6u10
    // - https://github.com/netty/netty/issues/203
    static {
        int selectorAutoRebuildThreshold = SystemPropertyUtil.getInt("io.netty.selectorAutoRebuildThreshold", 512);
        if (selectorAutoRebuildThreshold < MIN_PREMATURE_SELECTOR_RETURNS) {
            selectorAutoRebuildThreshold = 0;
        }

        SELECTOR_AUTO_REBUILD_THRESHOLD = selectorAutoRebuildThreshold;

        if (logger.isDebugEnabled()) {
            logger.debug("-Dio.netty.noKeySetOptimization: {}", DISABLE_KEY_SET_OPTIMIZATION);
            logger.debug("-Dio.netty.selectorAutoRebuildThreshold: {}", SELECTOR_AUTO_REBUILD_THRESHOLD);
        }
    }

    /**
     * The NIO {@link Selector}.
     * 在Java NIO中，Selector可以检测多个Channel上的事件（如连接就绪、读就绪、写就绪）。这样，一个线程就可以轮询多个Channel，
     * 而不需要为每个Channel都创建一个线程。这种机制在高并发应用中非常有用，因为它可以显著减少线程上下文切换的开销。
     */
    private Selector selector;
    // 原始的Selector，用于处理原生Selector的selectedKeys集合。
    private Selector unwrappedSelector;
    // 用于替换Selector内部selectedKeys和publicSelectedKeys的集合（优化用）
    private SelectedSelectionKeySet selectedKeys;

    // SelectorProvider
    // 用于创建选择器(Selector)、通道(SocketChannel, ServerSocketChannel)等。
    // 在Netty中，通过使用SelectorProvider，
    // 可以确保在不同的操作系统上使用合适的底层实现
    // （例如，在Windows上使用WindowsSelectorProvider，在Linux上使用EPollSelectorProvider等）。
    private final SelectorProvider provider;

    /**
     * Boolean that controls determines if a blocked Selector.select should
     * break out of its selection process. In our case we use a timeout for
     * the select method and the select method will block for that time unless
     * waken up.
     *
     * 用于控制Selector.select的超时唤醒
     */
    private final AtomicBoolean wakenUp = new AtomicBoolean();

    // 选择策略
    private final SelectStrategy selectStrategy;
    // 线程感知的执行器
    private final ThreadAwareExecutor executor;
    //已取消的键的数量
    private int cancelledKeys;
    // 是否需要再次选择
    private boolean needsToSelectAgain;

    private NioIoHandler(ThreadAwareExecutor executor, SelectorProvider selectorProvider,
                         SelectStrategy strategy) {
        this.executor = ObjectUtil.checkNotNull(executor, "executionContext");
        this.provider = ObjectUtil.checkNotNull(selectorProvider, "selectorProvider");
        this.selectStrategy = ObjectUtil.checkNotNull(strategy, "selectStrategy");
        final SelectorTuple selectorTuple = openSelector();
        this.selector = selectorTuple.selector;
        this.unwrappedSelector = selectorTuple.unwrappedSelector;
    }

    private static final class SelectorTuple {
        final Selector unwrappedSelector;
        final Selector selector;

        SelectorTuple(Selector unwrappedSelector) {
            this.unwrappedSelector = unwrappedSelector;
            this.selector = unwrappedSelector;
        }

        SelectorTuple(Selector unwrappedSelector, Selector selector) {
            this.unwrappedSelector = unwrappedSelector;
            this.selector = selector;
        }
    }

    // 原生的Selector内部使用HashSet来存储被选中的SelectionKey，
    // 而Netty使用数组实现的SelectedSelectionKeySet，这样可以减少迭代时的开销，提高性能。
    // 解决原生 Selector 在 selectedKeys() 集合操作上的性能瓶颈
    // 当优化失败时自动回退到标准实现
    private SelectorTuple openSelector() {
        // 1. 创建原生 Selector
        final Selector unwrappedSelector;
        try {
            unwrappedSelector = provider.openSelector();
        } catch (IOException e) {
            throw new ChannelException("failed to open a new selector", e);
        }
        // 2. 检查是否禁用优化
        if (DISABLE_KEY_SET_OPTIMIZATION) {
            return new SelectorTuple(unwrappedSelector);
        }
        // 3. 使用反射检查Selector的实现类是否为sun.nio.ch.SelectorImpl（或其子类）。
        Object maybeSelectorImplClass = AccessController.doPrivileged(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                try {
                    return Class.forName(
                            "sun.nio.ch.SelectorImpl",
                            false,
                            PlatformDependent.getSystemClassLoader());
                } catch (Throwable cause) {
                    return cause;
                }
            }
        });

        if (!(maybeSelectorImplClass instanceof Class) ||
                // ensure the current selector implementation is what we can instrument.
                !((Class<?>) maybeSelectorImplClass).isAssignableFrom(unwrappedSelector.getClass())) {
            // 如果不是，则返回原生Selector。
            if (maybeSelectorImplClass instanceof Throwable) {
                Throwable t = (Throwable) maybeSelectorImplClass;
                logger.trace("failed to instrument a special java.util.Set into: {}", unwrappedSelector, t);
            }
            return new SelectorTuple(unwrappedSelector);
        }

        final Class<?> selectorImplClass = (Class<?>) maybeSelectorImplClass;
        // 4. 创建一个优化的SelectedSelectionKeySet（这是Netty自定义的集合，用于替换Selector内部的两个selectedKeys集合）。
        final SelectedSelectionKeySet selectedKeySet = new SelectedSelectionKeySet();

        // 5. 通过反射（或Unsafe，如果可用）将Selector内部的selectedKeys和publicSelectedKeys替换为这个优化的集合。
        Object maybeException = AccessController.doPrivileged(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                try {
                    Field selectedKeysField = selectorImplClass.getDeclaredField("selectedKeys");
                    Field publicSelectedKeysField = selectorImplClass.getDeclaredField("publicSelectedKeys");

                    if (PlatformDependent.javaVersion() >= 9 && PlatformDependent.hasUnsafe()) {
                        // Let us try to use sun.misc.Unsafe to replace the SelectionKeySet.
                        // This allows us to also do this in Java9+ without any extra flags.
                        long selectedKeysFieldOffset = PlatformDependent.objectFieldOffset(selectedKeysField);
                        long publicSelectedKeysFieldOffset =
                                PlatformDependent.objectFieldOffset(publicSelectedKeysField);

                        if (selectedKeysFieldOffset != -1 && publicSelectedKeysFieldOffset != -1) {
                            PlatformDependent.putObject(
                                    unwrappedSelector, selectedKeysFieldOffset, selectedKeySet);
                            PlatformDependent.putObject(
                                    unwrappedSelector, publicSelectedKeysFieldOffset, selectedKeySet);
                            return null;
                        }
                        // We could not retrieve the offset, lets try reflection as last-resort.
                    }

                    Throwable cause = ReflectionUtil.trySetAccessible(selectedKeysField, true);
                    if (cause != null) {
                        return cause;
                    }
                    cause = ReflectionUtil.trySetAccessible(publicSelectedKeysField, true);
                    if (cause != null) {
                        return cause;
                    }

                    selectedKeysField.set(unwrappedSelector, selectedKeySet);
                    publicSelectedKeysField.set(unwrappedSelector, selectedKeySet);
                    return null;
                } catch (NoSuchFieldException | IllegalAccessException e) {
                    return e;
                }
            }
        });

        if (maybeException instanceof Exception) {
            selectedKeys = null;
            Exception e = (Exception) maybeException;
            logger.trace("failed to instrument a special java.util.Set into: {}", unwrappedSelector, e);
            return new SelectorTuple(unwrappedSelector);
        }
        selectedKeys = selectedKeySet;
        logger.trace("instrumented a special java.util.Set into: {}", unwrappedSelector);
        // 6. 如果替换成功，则返回一个包装了原生Selector和优化后的Selector的SelectorTuple；否则返回原生Selector。
        return new SelectorTuple(unwrappedSelector,
                new SelectedSelectionKeySetSelector(unwrappedSelector, selectedKeySet));
    }

    /**
     * Returns the {@link SelectorProvider} used by this {@link NioEventLoop} to obtain the {@link Selector}.
     */
    public SelectorProvider selectorProvider() {
        return provider;
    }

    Selector selector() {
        return selector;
    }

    int numRegistered() {
        return selector().keys().size() - cancelledKeys;
    }

    Set<SelectionKey> registeredSet() {
        return selector().keys();
    }

    /*
    * 重新构建Selector
    * */
    void rebuildSelector0() {
        final Selector oldSelector = selector;
        final SelectorTuple newSelectorTuple;

        if (oldSelector == null) {
            return;
        }

        try {
            // 1. 创建新Selector
            newSelectorTuple = openSelector();
        } catch (Exception e) {
            logger.warn("Failed to create a new Selector.", e);
            return;
        }

        // Register all channels to the new Selector.
        int nChannels = 0;
        // 2. 迁移所有Channel到新Selector
        for (SelectionKey key : oldSelector.keys()) {
            DefaultNioRegistration handle = (DefaultNioRegistration) key.attachment();
            try {
                if (!key.isValid() || key.channel().keyFor(newSelectorTuple.unwrappedSelector) != null) {
                    continue;
                }

                handle.register(newSelectorTuple.unwrappedSelector);
                nChannels++;
            } catch (Exception e) {
                logger.warn("Failed to re-register a NioHandle to the new Selector.", e);
                handle.cancel();
            }
        }

        // 3. 切换Selector引用
        selector = newSelectorTuple.selector;
        unwrappedSelector = newSelectorTuple.unwrappedSelector;

        try {
            // time to close the old selector as everything else is registered to the new one
            // 4. 关闭旧Selector
            oldSelector.close();
        } catch (Throwable t) {
            if (logger.isWarnEnabled()) {
                logger.warn("Failed to close the old Selector.", t);
            }
        }

        if (logger.isInfoEnabled()) {
            logger.info("Migrated " + nChannels + " channel(s) to the new Selector.");
        }
    }

    private static NioIoHandle nioHandle(IoHandle handle) {
        if (handle instanceof NioIoHandle) {
            return (NioIoHandle) handle;
        }
        throw new IllegalArgumentException("IoHandle of type " + StringUtil.simpleClassName(handle) + " not supported");
    }

    private static NioIoOps cast(IoOps ops) {
        if (ops instanceof NioIoOps) {
            return (NioIoOps) ops;
        }
        throw new IllegalArgumentException("IoOps of type " + StringUtil.simpleClassName(ops) + " not supported");
    }

    /*
    * DefaultNioRegistration
    *  * 用于包装NioIoHandle，实现IoRegistration接口
    *  * 维护SelectionKey生命周期
    *  * 处理interestOps更新
    *  * 取消注册时自动清理资源
    * */
    final class DefaultNioRegistration implements IoRegistration {
        private final AtomicBoolean canceled = new AtomicBoolean();
        private final NioIoHandle handle;
        private volatile SelectionKey key;

        DefaultNioRegistration(ThreadAwareExecutor executor, NioIoHandle handle, NioIoOps initialOps, Selector selector)
                throws IOException {
            this.handle = handle;
            // 底层实际调用JDK NIO的SelectableChannel.register()
            // 封装JDK原生API的异常处理
            key = handle.selectableChannel().register(selector, initialOps.value, this);
        }

        NioIoHandle handle() {
            return handle;
        }

        void register(Selector selector) throws IOException {
            SelectionKey newKey = handle.selectableChannel().register(selector, key.interestOps(), this);
            key.cancel();
            key = newKey;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T attachment() {
            return (T) key;
        }

        @Override
        public boolean isValid() {
            return !canceled.get() && key.isValid();
        }

        @Override
        public long submit(IoOps ops) {
            int v = cast(ops).value;
            key.interestOps(v);
            return v;
        }

        @Override
        public boolean cancel() {
            if (!canceled.compareAndSet(false, true)) {
                return false;
            }
            key.cancel();
            cancelledKeys++;
            if (cancelledKeys >= CLEANUP_INTERVAL) {
                cancelledKeys = 0;
                needsToSelectAgain = true;
            }
            return true;
        }

        void close() {
            cancel();
            try {
                handle.close();
            } catch (Exception e) {
                logger.debug("Exception during closing " + handle, e);
            }
        }

        void handle(int ready) {
            handle.handle(this, NioIoOps.eventOf(ready));
        }
    }

    @Override
    public IoRegistration register(IoHandle handle)
            throws Exception {
        // 通过nioHandle(handle)确保传入的handle是NioIoHandle实例
        NioIoHandle nioHandle = nioHandle(handle);// 类型检查与转换
        NioIoOps ops = NioIoOps.NONE;
        boolean selected = false;
        for (;;) {
            try {
                // 核心注册操作
                return new DefaultNioRegistration(executor, nioHandle, ops, unwrappedSelector());
            } catch (CancelledKeyException e) {
                // 使用无限循环处理CancelledKeyException
                if (!selected) {
                    // 首次异常时通过selectNow()强制更新Selector状态
                    // 强制刷新Selector状态
                    // Force the Selector to select now as the "canceled" SelectionKey may still be
                    // cached and not removed because no Select.select(..) operation was called yet.
                    selectNow();
                    selected = true;
                } else {
                    // We forced a select operation on the selector before but the SelectionKey is still cached
                    // for whatever reason. JDK bug ?
                    // 重试后仍失败则抛出异常
                    throw e;
                }
            }
        }
    }

    /*
    * run() 是 Netty NIO 事件循环的核心驱动方法，负责处理以下三大核心任务：
        事件选择：通过 Selector 监听 I/O 事件
        事件处理：执行就绪 Channel 的 I/O 操作
        异常恢复：处理 Selector 空轮询等异常情况
    * */
    @Override
    public int run(IoHandlerContext context) {
        int handled = 0;
        try {
            try {
                // 1. 选择策略决策（包含超时计算）
                switch (selectStrategy.calculateStrategy(selectNowSupplier, !context.canBlock())) {
                    case SelectStrategy.CONTINUE:
                        return 0;// 无需阻塞立即返回

                    case SelectStrategy.BUSY_WAIT:
                        // NIO 不支持忙等待
                        // fall-through to SELECT since the busy-wait is not supported with NIO

                    case SelectStrategy.SELECT:
                        // 核心选择逻辑
                        select(context, wakenUp.getAndSet(false));

                        // 'wakenUp.compareAndSet(false, true)' is always evaluated
                        // before calling 'selector.wakeup()' to reduce the wake-up
                        // overhead. (Selector.wakeup() is an expensive operation.)
                        //
                        // However, there is a race condition in this approach.
                        // The race condition is triggered when 'wakenUp' is set to
                        // true too early.
                        //
                        // 'wakenUp' is set to true too early if:
                        // 1) Selector is waken up between 'wakenUp.set(false)' and
                        //    'selector.select(...)'. (BAD)
                        // 2) Selector is waken up between 'selector.select(...)' and
                        //    'if (wakenUp.get()) { ... }'. (OK)
                        //
                        // In the first case, 'wakenUp' is set to true and the
                        // following 'selector.select(...)' will wake up immediately.
                        // Until 'wakenUp' is set to false again in the next round,
                        // 'wakenUp.compareAndSet(false, true)' will fail, and therefore
                        // any attempt to wake up the Selector will fail, too, causing
                        // the following 'selector.select(...)' call to block
                        // unnecessarily.
                        //
                        // To fix this problem, we wake up the selector again if wakenUp
                        // is true immediately after selector.select(...).
                        // It is inefficient in that it wakes up the selector for both
                        // the first case (BAD - wake-up required) and the second case
                        // (OK - no wake-up required).
                        // 2. 处理唤醒竞态条件
                        if (wakenUp.get()) {
                            selector.wakeup();// 双重检查唤醒
                        }
                        // fall through
                    default:
                }
            } catch (IOException e) {
                // If we receive an IOException here its because the Selector is messed up. Let's rebuild
                // the selector and retry. https://github.com/netty/netty/issues/8566
                // 3. Selector异常时重建
                rebuildSelector0();
                handleLoopException(e);
                return 0;
            }
            // 4. 处理就绪事件
            cancelledKeys = 0;
            needsToSelectAgain = false;
            // 事件分发核心
            handled = processSelectedKeys();
        } catch (Error e) {
            throw e;
        } catch (Throwable t) {
            // 5. 全局异常处理
            handleLoopException(t);
        }
        return handled;
    }

    private static void handleLoopException(Throwable t) {
        logger.warn("Unexpected exception in the selector loop.", t);

        // Prevent possible consecutive immediate failures that lead to
        // excessive CPU consumption.
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // Ignore.
        }
    }

    /*
    * - 事件分发枢纽
        处理所有就绪的 I/O 事件（OP_READ/OP_WRITE/OP_ACCEPT 等）
        将事件分发给对应的 Channel 处理
      -性能优化开关
        根据 selectedKeys 状态自动选择优化/标准实现
        优化路径比标准路径快 3-5 倍（Netty 官方基准测试）
    * */
    private int processSelectedKeys() {
        if (selectedKeys != null) {
            return processSelectedKeysOptimized();
        } else {
            return processSelectedKeysPlain(selector.selectedKeys());
        }
    }

    @Override
    public void destroy() {
        try {
            selector.close();
        } catch (IOException e) {
            logger.warn("Failed to close a selector.", e);
        }
    }

    private int processSelectedKeysPlain(Set<SelectionKey> selectedKeys) {
        // check if the set is empty and if so just return to not create garbage by
        // creating a new Iterator every time even if there is nothing to process.
        // See https://github.com/netty/netty/issues/597
        if (selectedKeys.isEmpty()) {
            return 0;
        }

        Iterator<SelectionKey> i = selectedKeys.iterator();
        int handled = 0;
        for (;;) {
            final SelectionKey k = i.next();
            i.remove();

            processSelectedKey(k);
            ++handled;

            if (!i.hasNext()) {
                break;
            }

            if (needsToSelectAgain) {
                selectAgain();
                selectedKeys = selector.selectedKeys();

                // Create the iterator again to avoid ConcurrentModificationException
                if (selectedKeys.isEmpty()) {
                    break;
                } else {
                    i = selectedKeys.iterator();
                }
            }
        }
        return handled;
    }

    private int processSelectedKeysOptimized() {
        int handled = 0;
        for (int i = 0; i < selectedKeys.size; ++i) {
            final SelectionKey k = selectedKeys.keys[i];
            // null out entry in the array to allow to have it GC'ed once the Channel close
            // See https://github.com/netty/netty/issues/2363
            // 显式置空帮助 GC
            selectedKeys.keys[i] = null;

            // 事件处理核心
            processSelectedKey(k);
            ++handled;

            if (needsToSelectAgain) {
                // 需要重新选择
                // null out entries in the array to allow to have it GC'ed once the Channel close
                // See https://github.com/netty/netty/issues/2363
                // 重置数组游标
                selectedKeys.reset(i + 1);

                // 重新执行 select
                selectAgain();
                // 重置循环索引
                i = -1;
            }
        }
        return handled;
    }

    private void processSelectedKey(SelectionKey k) {
        // 1. 获取关联的注册对象（包含Channel和事件处理器）
        final DefaultNioRegistration registration = (DefaultNioRegistration) k.attachment();
        // 2. 有效性校验（双重检查：原子标记 + SelectionKey自身状态）
        if (!registration.isValid()) {
            try {
                // 3. 关闭失效的Channel
                registration.handle.close();
            } catch (Exception e) {
                logger.debug("Exception during closing " + registration.handle, e);
            }
            return;
        }
        // 4. 事件处理核心（将就绪事件传递给Channel处理）
        registration.handle(k.readyOps());
    }

    @Override
    public void prepareToDestroy() {
        selectAgain();
        Set<SelectionKey> keys = selector.keys();
        Collection<DefaultNioRegistration> registrations = new ArrayList<>(keys.size());
        for (SelectionKey k: keys) {
            DefaultNioRegistration handle = (DefaultNioRegistration) k.attachment();
            registrations.add(handle);
        }

        for (DefaultNioRegistration reg: registrations) {
            reg.close();
        }
    }

    @Override
    public void wakeup() {
        if (!executor.isExecutorThread(Thread.currentThread()) && wakenUp.compareAndSet(false, true)) {
            selector.wakeup();
        }
    }

    @Override
    public boolean isCompatible(Class<? extends IoHandle> handleType) {
        return NioIoHandle.class.isAssignableFrom(handleType);
    }

    Selector unwrappedSelector() {
        return unwrappedSelector;
    }

    /*
    * 通过 Selector.select(timeout) 阻塞监听 I/O 事件
    * 动态计算超时时间（结合任务队列状态和调度任务）
    * 检测 JDK Selector 空轮询 BUG（Linux epoll 特定版本）
    * 通过 selectCnt 计数器触发 Selector 重建（阈值 512 次）
    * 使用 AtomicBoolean wakenUp 原子变量管理唤醒状态
    * 避免不必要的线程阻塞（通过 selectNow() 快速检查）
    * 精确控制阻塞时间（纳秒级时间计算）解决 wakeup() 与 select() 的时序竞态问题
    * */
    private void select(IoHandlerContext runner, boolean oldWakenUp) throws IOException {
        Selector selector = this.selector;
        try {
            // 选择计数器（用于检测空轮询）
            int selectCnt = 0;
            long currentTimeNanos = System.nanoTime();
            long selectDeadLineNanos = currentTimeNanos + runner.delayNanos(currentTimeNanos);

            for (;;) {
                // 1. 计算超时时间（精确到毫秒）
                long timeoutMillis = (selectDeadLineNanos - currentTimeNanos + 500000L) / 1000000L;
                // 2. 处理超时情况
                if (timeoutMillis <= 0) {
                    if (selectCnt == 0) {
                        // 首次立即返回
                        selector.selectNow();
                        selectCnt = 1;
                    }
                    break;
                }

                // If a task was submitted when wakenUp value was true, the task didn't get a chance to call
                // Selector#wakeup. So we need to check task queue again before executing select operation.
                // If we don't, the task might be pended until select operation was timed out.
                // It might be pended until idle timeout if IdleStateHandler existed in pipeline.
                // 3. 检查任务队列状态
                if (!runner.canBlock() && wakenUp.compareAndSet(false, true)) {
                    selector.selectNow();
                    selectCnt = 1;
                    break;
                }

                // 4. 执行阻塞式选择
                int selectedKeys = selector.select(timeoutMillis);
                // 递增选择次数
                selectCnt ++;

                // 5. 检查终止条件
                if (selectedKeys != 0 || oldWakenUp || wakenUp.get() || !runner.canBlock()) {
                    // - Selected something,
                    // - waken up by user, or
                    // - the task queue has a pending task.
                    // - a scheduled task is ready for processing
                    break;
                }
                // 6. 处理线程中断
                if (Thread.interrupted()) {
                    // Thread was interrupted so reset selected keys and break so we not run into a busy loop.
                    // As this is most likely a bug in the handler of the user or it's client library we will
                    // also log it.
                    //
                    // See https://github.com/netty/netty/issues/2426
                    if (logger.isDebugEnabled()) {
                        logger.debug("Selector.select() returned prematurely because " +
                                "Thread.currentThread().interrupt() was called. Use " +
                                "NioHandler.shutdownGracefully() to shutdown the NioHandler.");
                    }
                    selectCnt = 1;
                    break;
                }

                // 7. 检测空轮询（JDK BUG）
                long time = System.nanoTime();
                if (time - TimeUnit.MILLISECONDS.toNanos(timeoutMillis) >= currentTimeNanos) {
                    // timeoutMillis elapsed without anything selected.
                    // 正常超时
                    selectCnt = 1;
                } else if (SELECTOR_AUTO_REBUILD_THRESHOLD > 0 &&
                        selectCnt >= SELECTOR_AUTO_REBUILD_THRESHOLD) {
                    // The code exists in an extra method to ensure the method is not too big to inline as this
                    // branch is not very likely to get hit very frequently.
                    // 8. 触发Selector重建
                    selector = selectRebuildSelector(selectCnt);
                    selectCnt = 1;
                    break;
                }

                currentTimeNanos = time;
            }

            // 9. 记录异常选择次数
            if (selectCnt > MIN_PREMATURE_SELECTOR_RETURNS) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Selector.select() returned prematurely {} times in a row for Selector {}.",
                            selectCnt - 1, selector);
                }
            }
        } catch (CancelledKeyException e) {
            if (logger.isDebugEnabled()) {
                logger.debug(CancelledKeyException.class.getSimpleName() + " raised by a Selector {} - JDK bug?",
                        selector, e);
            }
            // Harmless exception - log anyway
        }
    }

    int selectNow() throws IOException {
        try {
            return selector.selectNow();
        } finally {
            // restore wakeup state if needed
            if (wakenUp.get()) {
                selector.wakeup();
            }
        }
    }

    private Selector selectRebuildSelector(int selectCnt) throws IOException {
        // The selector returned prematurely many times in a row.
        // Rebuild the selector to work around the problem.
        logger.warn(
                "Selector.select() returned prematurely {} times in a row; rebuilding Selector {}.",
                selectCnt, selector);

        rebuildSelector0();
        Selector selector = this.selector;

        // Select again to populate selectedKeys.
        selector.selectNow();
        return selector;
    }

    private void selectAgain() {
        needsToSelectAgain = false;
        try {
            selector.selectNow();
        } catch (Throwable t) {
            logger.warn("Failed to update SelectionKeys.", t);
        }
    }

    /**
     * Returns a new {@link IoHandlerFactory} that creates {@link NioIoHandler} instances
     *
     * @return factory                  the {@link IoHandlerFactory}.
     */
    public static IoHandlerFactory newFactory() {
        return newFactory(SelectorProvider.provider(), DefaultSelectStrategyFactory.INSTANCE);
    }

    /**
     * Returns a new {@link IoHandlerFactory} that creates {@link NioIoHandler} instances.
     *
     * @param selectorProvider          the {@link SelectorProvider} to use.
     * @return factory                  the {@link IoHandlerFactory}.
     */
    public static IoHandlerFactory newFactory(SelectorProvider selectorProvider) {
        return newFactory(selectorProvider, DefaultSelectStrategyFactory.INSTANCE);
    }

    /**
     * Returns a new {@link IoHandlerFactory} that creates {@link NioIoHandler} instances.
     *
     * @param selectorProvider          the {@link SelectorProvider} to use.
     * @param selectStrategyFactory     the {@link SelectStrategyFactory} to use.
     * @return factory                  the {@link IoHandlerFactory}.
     */
    public static IoHandlerFactory newFactory(final SelectorProvider selectorProvider,
                                              final SelectStrategyFactory selectStrategyFactory) {
        ObjectUtil.checkNotNull(selectorProvider, "selectorProvider");
        ObjectUtil.checkNotNull(selectStrategyFactory, "selectStrategyFactory");
        return context ->  new NioIoHandler(context, selectorProvider, selectStrategyFactory.newSelectStrategy());
    }
}
