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
package org.apache.rocketmq.store;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

/**
 * 临时存储池
 */
public class TransientStorePool {
    /*
        Java NIO中可以使用内存映射机制将文件系统中的文件映射到内存中，但是这部分映射的内存不是常驻内存的，
        可以被置换到交换内存中。

        RocketMQ为了提高消息发送性能，引入了内存锁定机制，将文件映射到内存后并锁定，保证这些映射的内存始终
        在内存中。

        transientStorePoolEnable参数为true的时候开启临时存储池。

        如果开启了transientStorePoolEnable，则消息写入是写writeBuffer，如果不启用则消息写入mappedByteBuffer。
        而消息的读取则是始终从mappedByteBuffer中读取。相当于在开启了transientStorePoolEnable之后，有了能读写
        分离的效果。

        如果不开启transientStorePoolEnable，则使用的是mmap(内存映射） + PageCache的方式，读写消息都是走PageCache，
        读写都在PageCache里面会有锁的问题，并发读写的情况下出现缺页中断增加，内存加锁污染页的回写。

        如果开启transientStorePoolEnable，使用的是DirectByteBuffer（堆外内存） + PageCache的方式，读写消息分离，写
        消息是在堆外内存中，读消息时走PageCache，这样可以减少mmap出现的问题。
     */
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 可用的缓冲区个数
     */
    private final int poolSize;

    /**
     * 每个缓冲区的大小
     */
    private final int fileSize;

    /**
     * 存放缓冲区的队列
     */
    private final Deque<ByteBuffer> availableBuffers;

    /**
     * 消息存储的配置
     */
    private final MessageStoreConfig storeConfig;

    public TransientStorePool(final MessageStoreConfig storeConfig) {
        this.storeConfig = storeConfig;
        this.poolSize = storeConfig.getTransientStorePoolSize();
        this.fileSize = storeConfig.getMappedFileSizeCommitLog();
        this.availableBuffers = new ConcurrentLinkedDeque<>();
    }

    /**
     * It's a heavy init method.
     * 初始化临时存储池，会申请堆外内存、使用系统调用mlock锁定内存，防止调度到交换空间
     */
    public void init() {
        for (int i = 0; i < poolSize; i++) {
            // 直接分配堆外内存
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(fileSize);

            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);
            // 使用系统调用mlock锁住物理内存，防止将这些内存调度到交换空间
            LibC.INSTANCE.mlock(pointer, new NativeLong(fileSize));

            // 添加到缓冲区队列中缓存
            availableBuffers.offer(byteBuffer);
        }
    }

    /**
     * 销毁缓冲区内存
     */
    public void destroy() {
        for (ByteBuffer byteBuffer : availableBuffers) {
            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);
            // 解除锁定
            LibC.INSTANCE.munlock(pointer, new NativeLong(fileSize));
        }
    }

    /**
     * 归还ByteBuffer
     * @param byteBuffer
     */
    public void returnBuffer(ByteBuffer byteBuffer) {
        // position设为0，可以从头开始写
        byteBuffer.position(0);
        // 写的limit设置为最大
        byteBuffer.limit(fileSize);
        // 添加到缓冲区队列中
        this.availableBuffers.offerFirst(byteBuffer);
    }

    /**
     * 申请缓冲区
     * @return
     */
    public ByteBuffer borrowBuffer() {
        // 从缓冲区队列中获取一个缓冲区
        ByteBuffer buffer = availableBuffers.pollFirst();
        if (availableBuffers.size() < poolSize * 0.4) {
            log.warn("TransientStorePool only remain {} sheets.", availableBuffers.size());
        }
        return buffer;
    }

    public int availableBufferNums() {
        if (storeConfig.isTransientStorePoolEnable()) {
            return availableBuffers.size();
        }
        return Integer.MAX_VALUE;
    }
}
