/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store.ha;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class WaitNotifyObject {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 所有在阻塞等待的线程
     */
    protected final ConcurrentHashMap<Long/* thread id */, AtomicBoolean/* notified */> waitingThreadTable =
        new ConcurrentHashMap<Long, AtomicBoolean>(16);

    /**
     * 用来标识线程有没有被唤醒
     */
    protected AtomicBoolean hasNotified = new AtomicBoolean(false);

    /**
     * 唤醒线程
     */
    public void wakeup() {
        // 先将标志置为已通知，告诉其他的线程要进行唤醒了
        boolean needNotify = hasNotified.compareAndSet(false, true);
        if (needNotify) {
            synchronized (this) {
                // 进行唤醒
                this.notify();
            }
        }
    }

    /**
     * 让线程阻塞等待指定的时间，到了时间后继续运行，或者调用了wakeup进行唤醒
     * @param interval
     */
    protected void waitForRunning(long interval) {
        /*
            如果cas能成功，说明线程已经被唤醒了，也就是刚好有调用wakeup方法，
            此时当前方法就不能再继续进行阻塞等待的操作了，此时直接执行唤醒后要执行的方法
         */
        if (this.hasNotified.compareAndSet(true, false)) {
            // 等待结束了
            this.onWaitEnd();
            return;
        }

        // 加锁调用wait方法进行阻塞
        synchronized (this) {
            try {
                // 加锁后再次看看有没有被唤醒。
                if (this.hasNotified.compareAndSet(true, false)) {
                    // 等待结束了
                    this.onWaitEnd();
                    return;
                }

                // 执行等待
                this.wait(interval);
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
            } finally {
                this.hasNotified.set(false);
                this.onWaitEnd();
            }
        }
    }

    protected void onWaitEnd() {
    }

    /**
     * 唤醒所有的线程，比如一个master可能有多个slave，就会有多个salve连接到master上，
     * 当master没有新消息的时候，可能多个同步到slave的服务就会阻塞等待，如果有新消息到来，
     * 就可以使用wakeupAll唤醒所有阻塞等待的线程了。
     */
    public void wakeupAll() {
        boolean needNotify = false;
        for (Map.Entry<Long,AtomicBoolean> entry : this.waitingThreadTable.entrySet()) {
            if (entry.getValue().compareAndSet(false, true)) {
                needNotify = true;
            }
        }
        if (needNotify) {
            synchronized (this) {
                this.notifyAll();
            }
        }
    }

    /**
     * 所有的线程都进行阻塞等待，直到超时，或者被唤醒
     * 比如一个master可能有多个slave，就会有多个salve连接到master上，
     * 当master没有新消息的时候，可能多个同步到slave的服务就会阻塞等待，
     * 就可以调用allWaitForRunning让所有服务进行阻塞等待
     * @param interval
     */
    public void allWaitForRunning(long interval) {
        long currentThreadId = Thread.currentThread().getId();
        AtomicBoolean notified = this.waitingThreadTable.computeIfAbsent(currentThreadId, k -> new AtomicBoolean(false));
        if (notified.compareAndSet(true, false)) {
            this.onWaitEnd();
            return;
        }
        synchronized (this) {
            try {
                if (notified.compareAndSet(true, false)) {
                    this.onWaitEnd();
                    return;
                }
                this.wait(interval);
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
            } finally {
                notified.set(false);
                this.onWaitEnd();
            }
        }
    }

    public void removeFromWaitingThreadTable() {
        long currentThreadId = Thread.currentThread().getId();
        synchronized (this) {
            this.waitingThreadTable.remove(currentThreadId);
        }
    }
}
