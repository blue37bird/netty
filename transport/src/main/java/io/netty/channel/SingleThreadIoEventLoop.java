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
package io.netty.channel;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.RejectedExecutionHandler;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.SystemPropertyUtil;

import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link IoEventLoop} implementation that execute all its submitted tasks in a single thread using the provided
 * {@link IoHandler}.
 */
/**
 * 单线程IO事件循环核心实现（Reactor模式实现）
 *
 * 继承体系：
 * SingleThreadEventLoop → IoEventLoop
 *
 * 设计特点：
 * 1. 单线程执行模型：一个EventLoop对应一个线程
 * 2. IO与任务混合调度：平衡IO处理与异步任务执行
 * 3. 可扩展的IO处理：通过IoHandler支持不同IO模型（NIO/Epoll等）
 */
public class SingleThreadIoEventLoop extends SingleThreadEventLoop implements IoEventLoop {

    // TODO: Is this a sensible default ?
    // 最大任务处理时间（防止任务处理饥饿IO操作）
    private static final long DEFAULT_MAX_TASK_PROCESSING_QUANTUM_NS = TimeUnit.MILLISECONDS.toNanos(Math.max(100,
            SystemPropertyUtil.getInt("io.netty.eventLoop.maxTaskProcessingQuantumMs", 1000)));

    private final long maxTaskProcessingQuantumNs;

    // IO处理器上下文（提供线程安全的状态访问）
    // `IoHandlerContext` 的作用是作为事件循环（EventLoop）与IO处理器（IoHandler）之间的一个桥梁，
    // 提供事件循环当前的状态信息，以便IO处理器根据这些信息做出决策（例如，在等待IO事件时是否阻塞、阻塞多长时间等）。
    private final IoHandlerContext context = new IoHandlerContext() {
        /*
        * `canBlock()`: 判断是否允许阻塞（当没有任务时）。
        * 在实现中，它检查当前事件循环中是否有普通任务或定时任务，如果没有，则允许阻塞。
        * */
        @Override
        public boolean canBlock() {// 判断是否允许阻塞（当无任务时）
            assert inEventLoop();
            return !hasTasks() && !hasScheduledTasks();
        }

        /*
        *  计算距离下一个定时任务执行还有多少纳秒。它调用了外部类（SingleThreadIoEventLoop）的 `delayNanos` 方法。
        * */
        @Override
        public long delayNanos(long currentTimeNanos) {
            assert inEventLoop();
            return SingleThreadIoEventLoop.this.delayNanos(currentTimeNanos);
        }

        /*
        * 返回下一个定时任务的截止时间（纳秒）。同样，它调用了外部类的 `deadlineNanos` 方法。
        * */
        @Override
        public long deadlineNanos() {
            assert inEventLoop();
            return SingleThreadIoEventLoop.this.deadlineNanos();
        }
    };

    // 实际IO处理器（NIO/Epoll等实现）
    /*
    IoHandler是Netty事件循环中用于处理IO操作的核心接口，它负责：
            1. 初始化IO资源（如Selector）。
            2. 运行IO事件循环（处理就绪的IO事件）。
            3. 注册IO句柄（如Socket）并返回注册凭证。
            4. 提供唤醒机制，以便在阻塞的IO操作中及时响应新任务。
            5. 在事件循环结束时销毁资源。
    */
    private final IoHandler ioHandler;

    // 活跃注册数计数器
    private final AtomicInteger numRegistrations = new AtomicInteger();

    /**
     *  Creates a new instance
     *
     * @param parent            the parent that holds this {@link IoEventLoop}.
     * @param threadFactory     the {@link ThreadFactory} that is used to create the underlying {@link Thread}.
     * @param ioHandlerFactory  the {@link IoHandlerFactory} that should be used to obtain {@link IoHandler} to
     *                          handle IO.
     */
    public SingleThreadIoEventLoop(IoEventLoopGroup parent, ThreadFactory threadFactory,
                                   IoHandlerFactory ioHandlerFactory) {
        super(parent, threadFactory, false, true);
        this.maxTaskProcessingQuantumNs = DEFAULT_MAX_TASK_PROCESSING_QUANTUM_NS;
        this.ioHandler = ObjectUtil.checkNotNull(ioHandlerFactory, "ioHandlerFactory").newHandler(this);
    }

    /**
     *  Creates a new instance
     *
     * @param parent            the parent that holds this {@link IoEventLoop}.
     * @param executor          the {@link Executor} that is used for dispatching the work.
     * @param ioHandlerFactory  the {@link IoHandlerFactory} that should be used to obtain {@link IoHandler} to
     *                          handle IO.
     */
    public SingleThreadIoEventLoop(IoEventLoopGroup parent, Executor executor, IoHandlerFactory ioHandlerFactory) {
        super(parent, executor, false, true);
        this.maxTaskProcessingQuantumNs = DEFAULT_MAX_TASK_PROCESSING_QUANTUM_NS;
        this.ioHandler = ObjectUtil.checkNotNull(ioHandlerFactory, "ioHandlerFactory").newHandler(this);
    }

    /**
     *  Creates a new instance
     *
     * @param parent                        the parent that holds this {@link IoEventLoop}.
     * @param threadFactory                 the {@link ThreadFactory} that is used to create the underlying
     *                                      {@link Thread}.
     * @param ioHandlerFactory              the {@link IoHandlerFactory} that should be used to obtain {@link IoHandler}
     *                                      to handle IO.
     * @param maxPendingTasks               the maximum pending tasks that are allowed before
     *                                      {@link RejectedExecutionHandler#rejected(Runnable,
     *                                          SingleThreadEventExecutor)}
     *                                      is called to handle it.
     * @param rejectedExecutionHandler      the {@link RejectedExecutionHandler} that handles when more tasks are added
     *                                      then allowed per {@code maxPendingTasks}.
     * @param maxTaskProcessingQuantumMs    the maximum number of milliseconds that will be spent to run tasks before
     *                                      trying to run IO again.
     */
    public SingleThreadIoEventLoop(IoEventLoopGroup parent, ThreadFactory threadFactory,
                                   IoHandlerFactory ioHandlerFactory, int maxPendingTasks,
                                   RejectedExecutionHandler rejectedExecutionHandler, long maxTaskProcessingQuantumMs) {
        super(parent, threadFactory, false, true, maxPendingTasks, rejectedExecutionHandler);
        this.maxTaskProcessingQuantumNs =
                ObjectUtil.checkPositiveOrZero(maxTaskProcessingQuantumMs, "maxTaskProcessingQuantumMs") == 0 ?
                        DEFAULT_MAX_TASK_PROCESSING_QUANTUM_NS :
                        TimeUnit.MILLISECONDS.toNanos(maxTaskProcessingQuantumMs);
        this.ioHandler = ObjectUtil.checkNotNull(ioHandlerFactory, "ioHandlerFactory").newHandler(this);
    }

    /**
     *  Creates a new instance
     *
     * @param parent                        the parent that holds this {@link IoEventLoop}.
     * @param ioHandlerFactory              the {@link IoHandlerFactory} that should be used to obtain {@link IoHandler}
     *                                      to handle IO.
     * @param maxPendingTasks               the maximum pending tasks that are allowed before
     *                                      {@link RejectedExecutionHandler#rejected(Runnable,
     *                                          SingleThreadEventExecutor)}
     *                                      is called to handle it.
     * @param rejectedExecutionHandler      the {@link RejectedExecutionHandler} that handles when more tasks are added
     *                                      then allowed per {@code maxPendingTasks}.
     * @param maxTaskProcessingQuantumMs    the maximum number of milliseconds that will be spent to run tasks before
     *                                      trying to run IO again.
     */
    public SingleThreadIoEventLoop(IoEventLoopGroup parent, Executor executor,
                                   IoHandlerFactory ioHandlerFactory, int maxPendingTasks,
                                   RejectedExecutionHandler rejectedExecutionHandler,
                                   long maxTaskProcessingQuantumMs) {
        super(parent, executor, false, true, maxPendingTasks, rejectedExecutionHandler);
        this.maxTaskProcessingQuantumNs =
                ObjectUtil.checkPositiveOrZero(maxTaskProcessingQuantumMs, "maxTaskProcessingQuantumMs") == 0 ?
                        DEFAULT_MAX_TASK_PROCESSING_QUANTUM_NS :
                        TimeUnit.MILLISECONDS.toNanos(maxTaskProcessingQuantumMs);
        this.ioHandler = ObjectUtil.checkNotNull(ioHandlerFactory, "ioHandlerFactory").newHandler(this);
    }

    /**
     *
     *  Creates a new instance
     *
     * @param parent                    the parent that holds this {@link IoEventLoop}.
     * @param executor                  the {@link Executor} that is used for dispatching the work.
     * @param ioHandlerFactory          the {@link IoHandlerFactory} that should be used to obtain {@link IoHandler}
     *                                  to handle IO.
     * @param taskQueue                 the {@link Queue} used for storing pending tasks.
     * @param tailTaskQueue             the {@link Queue} used for storing tail pending tasks.
     * @param rejectedExecutionHandler  the {@link RejectedExecutionHandler} that handles when more tasks are added
     *                                  then allowed.
     */
    protected SingleThreadIoEventLoop(IoEventLoopGroup parent, Executor executor,
                                      IoHandlerFactory ioHandlerFactory, Queue<Runnable> taskQueue,
                                      Queue<Runnable> tailTaskQueue,
                                      RejectedExecutionHandler rejectedExecutionHandler) {
        super(parent, executor, false, true, taskQueue, tailTaskQueue, rejectedExecutionHandler);
        this.maxTaskProcessingQuantumNs = DEFAULT_MAX_TASK_PROCESSING_QUANTUM_NS;
        this.ioHandler = ObjectUtil.checkNotNull(ioHandlerFactory, "ioHandlerFactory").newHandler(this);
    }

    /**
     * 核心事件循环逻辑
     *
     * 执行流程：
     * 1. 初始化IO处理器
     * 2. 循环处理：
     *    a. 处理IO事件
     *    b. 处理异步任务（带时间限制）
     * 3. 关闭前准备
     */
    @Override
    protected void run() {
        assert inEventLoop();
        ioHandler.initialize();// 初始化IO组件
        do {
            // 处理IO事件（返回处理的IO句柄数）
            runIo();
            // isShuttingDown() 是事件循环状态检查方法，主要用于判断当前事件循环是否处于关闭流程中。
            // 返回 true 表示事件循环已开始关闭流程（SHUTTING_DOWN 或更高状态）
            // 返回 false 表示事件循环仍在正常运行
            // 实现在父类 SingleThreadEventExecutor 的实现
            if (isShuttingDown()) {// 检查关闭状态
                ioHandler.prepareToDestroy();// 准备销毁IO资源
            }
            // Now run all tasks for the maximum configured amount of time before trying to run IO again.
            // 处理异步任务（带时间限制）
            runAllTasks(maxTaskProcessingQuantumNs);

            // We should continue with our loop until we either confirmed a shutdown or we can suspend it.
            // confirmShutdown方法返回true表示已经完成关闭，可以退出循环；返回false表示还需要继续处理（可能还有任务没执行完，或者还需要执行其他关闭步骤）。
            // canSuspend 是事件循环生命周期管理的安全阀，确保仅在无活跃IO连接时挂起线程，防止资源泄漏和状态不一致问题。
        } while (!confirmShutdown() && !canSuspend());
    }

    protected final IoHandler ioHandler() {
        return ioHandler;
    }

    @Override
    protected boolean canSuspend(int state) {
        // We should only allow to suspend if there are no registrations on this loop atm.
        return super.canSuspend(state) && numRegistrations.get() == 0;
    }

    /**
     * Called when IO will be processed for all the {@link IoHandle}s on this {@link SingleThreadIoEventLoop}.
     * This method returns the number of {@link IoHandle}s for which IO was processed.
     *
     * This method must be called from the {@link EventLoop} thread.
     */
    protected int runIo() {
        assert inEventLoop();
        // 委托给具体IO实现
        return ioHandler.run(context);
    }

    @Override
    public IoEventLoop next() {
        return this;
    }

    @Override
    public final Future<IoRegistration> register(final IoHandle handle) {
        Promise<IoRegistration> promise = newPromise();
        if (inEventLoop()) {
            registerForIo0(handle, promise);
        } else {
            execute(() -> registerForIo0(handle, promise));
        }

        return promise;
    }

    private void registerForIo0(final IoHandle handle, Promise<IoRegistration> promise) {
        assert inEventLoop();// 确保在事件循环线程执行
        final IoRegistration registration;
        try {
            // 1. 通过IO处理器进行实际注册
            registration = ioHandler.register(handle);
        } catch (Exception e) {
            // 2. 注册失败处理
            promise.setFailure(e);
            return;
        }
        // 3. 更新注册计数器
        numRegistrations.incrementAndGet();
        // 4. 返回包装后的注册凭证
        promise.setSuccess(new IoRegistrationWrapper(registration));
    }

    @Override
    protected final void wakeup(boolean inEventLoop) {
        ioHandler.wakeup();
    }

    @Override
    protected final void cleanup() {
        assert inEventLoop();
        ioHandler.destroy();
    }

    @Override
    public boolean isCompatible(Class<? extends IoHandle> handleType) {
        return ioHandler.isCompatible(handleType);
    }

    @Override
    public boolean isIoType(Class<? extends IoHandler> handlerType) {
        return ioHandler.getClass().equals(handlerType);
    }

    @Override
    protected Queue<Runnable> newTaskQueue(int maxPendingTasks) {
        return newTaskQueue0(maxPendingTasks);
    }

    protected static Queue<Runnable> newTaskQueue0(int maxPendingTasks) {
        // This event loop never calls takeTask()
        return maxPendingTasks == Integer.MAX_VALUE ? PlatformDependent.<Runnable>newMpscQueue()
                : PlatformDependent.<Runnable>newMpscQueue(maxPendingTasks);
    }

    private final class IoRegistrationWrapper implements IoRegistration {
        private final IoRegistration registration;
        IoRegistrationWrapper(IoRegistration registration) {
            this.registration = registration;
        }

        @Override
        public <T> T attachment() {
            return registration.attachment();
        }

        @Override
        public long submit(IoOps ops) {
            return registration.submit(ops);
        }

        @Override
        public boolean isValid() {
            return registration.isValid();
        }

        @Override
        public boolean cancel() {
            if (registration.cancel()) {
                numRegistrations.decrementAndGet();
                return true;
            }
            return false;
        }
    }
}
