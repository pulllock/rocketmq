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

    protected final ConcurrentHashMap<Long/* thread id */, AtomicBoolean/* notified */> waitingThreadTable =
        new ConcurrentHashMap<Long, AtomicBoolean>(16);

    /**
     * 当前对象有没有被通知过
     */
    protected AtomicBoolean hasNotified = new AtomicBoolean(false);

    /**
     * 唤醒操作
     */
    public void wakeup() {
        /*
            一般情况下，如果要唤醒，说明当前的状态正在等待，此时hasNotified期望的值是false，
            此时cas能成功，cas成功后调用notify方法进行唤醒操作。

            如果此时hasNotified是true，说明有其他线程进行了唤醒，不需要再次唤醒。
         */
        boolean needNotify = hasNotified.compareAndSet(false, true);
        if (needNotify) {
            synchronized (this) {
                // 唤醒操作
                this.notify();
            }
        }
    }

    /**
     * 等待运行，带超时时间
     * @param interval
     */
    protected void waitForRunning(long interval) {
        /*
            hasNotified初始化为false，如果是第一次进来这里cas不会成功。
            如果已经被唤醒，则这里可以cas成功，说明当前线程要进行等待的时候，
            有其他的线程已经唤醒了，所以这里可以成功。

            这里是进行等待的操作，所以期望
         */
        if (this.hasNotified.compareAndSet(true, false)) {
            // 等待结束了
            this.onWaitEnd();
            return;
        }

        // 加锁调用wait方法
        synchronized (this) {
            try {
                /*
                    加锁后再次看看有没有被唤醒。

                    hasNotified初始化为false，如果是第一次进来这里cas不会成功。
                    如果已经被唤醒，则这里可以cas成功，说明当前线程要进行等待的时候，
                    有其他的线程已经唤醒了，所以这里可以成功。
                 */
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
