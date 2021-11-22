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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.SelectMappedBufferResult;

/**
 * HA master服务端：
 * - 接受slave上报偏移量的请求
 * - 向slave发送数据
 */
public class HAConnection {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 高可用服务
     */
    private final HAService haService;

    /**
     * 连接的Channel
     */
    private final SocketChannel socketChannel;

    /**
     * 客户端地址
     */
    private final String clientAddr;

    /**
     * HA服务的master节点进行写的服务
     */
    private WriteSocketService writeSocketService;

    /**
     * HA服务的master节点进行读的服务
     */
    private ReadSocketService readSocketService;

    /**
     * 从服务器请求拉取数据的偏移量
     */
    private volatile long slaveRequestOffset = -1;

    /**
     * 从服务器反馈已拉取完的数据偏移量
     */
    private volatile long slaveAckOffset = -1;

    public HAConnection(final HAService haService, final SocketChannel socketChannel) throws IOException {
        // 高可用服务
        this.haService = haService;

        // 连接的Channel
        this.socketChannel = socketChannel;

        // 客户端地址
        this.clientAddr = this.socketChannel.socket().getRemoteSocketAddress().toString();
        this.socketChannel.configureBlocking(false);
        this.socketChannel.socket().setSoLinger(false, -1);
        this.socketChannel.socket().setTcpNoDelay(true);
        this.socketChannel.socket().setReceiveBufferSize(1024 * 64);
        this.socketChannel.socket().setSendBufferSize(1024 * 64);

        // HA服务的master节点进行写的服务
        this.writeSocketService = new WriteSocketService(this.socketChannel);

        // HA服务的master节点进行读的服务
        this.readSocketService = new ReadSocketService(this.socketChannel);
        this.haService.getConnectionCount().incrementAndGet();
    }

    /**
     * 启动处理读和写的服务
     */
    public void start() {
        // 处理读的服务启动
        this.readSocketService.start();

        // 处理写的服务启动
        this.writeSocketService.start();
    }

    public void shutdown() {
        this.writeSocketService.shutdown(true);
        this.readSocketService.shutdown(true);
        this.close();
    }

    public void close() {
        if (this.socketChannel != null) {
            try {
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }
        }
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    /**
     * 读取slave上报的偏移量请求
     */
    class ReadSocketService extends ServiceThread {
        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024;
        private final Selector selector;
        private final SocketChannel socketChannel;
        private final ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
        private int processPosition = 0;
        private volatile long lastReadTimestamp = System.currentTimeMillis();

        public ReadSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = RemotingUtil.openSelector();
            this.socketChannel = socketChannel;
            // 监听读事件
            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
            this.setDaemon(true);
        }

        /**
         * 每隔1s处理一次请求
         * 解析slave上报偏移量的请求
         */
        @Override
        public void run() {
            HAConnection.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 带超时的select，master等待slave上报偏移量
                    this.selector.select(1000);
                    // 处理读事件，解析slave上报偏移量的请求
                    boolean ok = this.processReadEvent();
                    if (!ok) {
                        HAConnection.log.error("processReadEvent error");
                        break;
                    }

                    long interval = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now() - this.lastReadTimestamp;
                    if (interval > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaHousekeepingInterval()) {
                        log.warn("ha housekeeping, found this connection[" + HAConnection.this.clientAddr + "] expired, " + interval);
                        break;
                    }
                } catch (Exception e) {
                    HAConnection.log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }

            this.makeStop();

            writeSocketService.makeStop();

            haService.removeConnection(HAConnection.this);

            HAConnection.this.haService.getConnectionCount().decrementAndGet();

            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                this.selector.close();
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }

            HAConnection.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return ReadSocketService.class.getSimpleName();
        }

        private boolean processReadEvent() {
            int readSizeZeroTimes = 0;

            if (!this.byteBufferRead.hasRemaining()) {
                this.byteBufferRead.flip();
                this.processPosition = 0;
            }

            while (this.byteBufferRead.hasRemaining()) {
                try {
                    int readSize = this.socketChannel.read(this.byteBufferRead);
                    if (readSize > 0) {
                        readSizeZeroTimes = 0;
                        this.lastReadTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                        if ((this.byteBufferRead.position() - this.processPosition) >= 8) {
                            int pos = this.byteBufferRead.position() - (this.byteBufferRead.position() % 8);
                            // salve上报请求中的偏移量
                            long readOffset = this.byteBufferRead.getLong(pos - 8);
                            this.processPosition = pos;

                            // slave上报的偏移量
                            HAConnection.this.slaveAckOffset = readOffset;
                            if (HAConnection.this.slaveRequestOffset < 0) {
                                HAConnection.this.slaveRequestOffset = readOffset;
                                log.info("slave[" + HAConnection.this.clientAddr + "] request offset " + readOffset);
                            }

                            /*
                                notifyTransferSome在master是同步类型的节点时才有用，用来进行相应的唤醒操作。
                                如果是异步的master则没有要求。
                             */
                            HAConnection.this.haService.notifyTransferSome(HAConnection.this.slaveAckOffset);
                        }
                    } else if (readSize == 0) {
                        if (++readSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        log.error("read socket[" + HAConnection.this.clientAddr + "] < 0");
                        return false;
                    }
                } catch (IOException e) {
                    log.error("processReadEvent exception", e);
                    return false;
                }
            }

            return true;
        }
    }

    /**
     * 向slave发送数据
     */
    class WriteSocketService extends ServiceThread {
        private final Selector selector;
        private final SocketChannel socketChannel;

        private final int headerSize = 8 + 4;

        /**
         * 发送消息的头部数据
         * 前八个字节放的是发送数据的起始偏移量
         * 后四个字节放的是发送数据的大小
         */
        private final ByteBuffer byteBufferHeader = ByteBuffer.allocate(headerSize);

        /**
         * 下一次同步数据的起始偏移量
         */
        private long nextTransferFromWhere = -1;

        /**
         * master每次向slave同步的数据
         */
        private SelectMappedBufferResult selectMappedBufferResult;

        /**
         * 上次同步是否完成
         */
        private boolean lastWriteOver = true;

        /**
         * 上一次同步的时间
         */
        private long lastWriteTimestamp = System.currentTimeMillis();

        public WriteSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = RemotingUtil.openSelector();
            this.socketChannel = socketChannel;

            // 监听写事件
            this.socketChannel.register(this.selector, SelectionKey.OP_WRITE);
            this.setDaemon(true);
        }

        @Override
        public void run() {
            HAConnection.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 带超时的select
                    this.selector.select(1000);

                    // slaveRequestOffset只有在初始户的时候为-1
                    if (-1 == HAConnection.this.slaveRequestOffset) {
                        Thread.sleep(10);
                        continue;
                    }

                    /*
                        nextTransferFromWhere只有在初始化的时候为-1，如果是-1说明是slave和master刚建立
                        连接，还没有进行过同步。
                     */
                    if (-1 == this.nextTransferFromWhere) {
                        // slaveRequestOffset=0说明slave上没有任何的commitlog数据
                        if (0 == HAConnection.this.slaveRequestOffset) {
                            // master获取最大的偏移量，如果slave是新的，没有任何的commitlog数据，则master只会从最后一个文件的开始的地方进行同步
                            long masterOffset = HAConnection.this.haService.getDefaultMessageStore().getCommitLog().getMaxOffset();
                            masterOffset =
                                masterOffset
                                    - (masterOffset % HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig()
                                    .getMappedFileSizeCommitLog());

                            if (masterOffset < 0) {
                                masterOffset = 0;
                            }

                            // 下一次要同步的起始偏移量
                            this.nextTransferFromWhere = masterOffset;
                        } else {
                            // 如果slave上本身就有commitlog数据，则从slave上报的偏移量的位置开始同步
                            this.nextTransferFromWhere = HAConnection.this.slaveRequestOffset;
                        }

                        log.info("master transfer data from " + this.nextTransferFromWhere + " to slave[" + HAConnection.this.clientAddr
                            + "], and slave request " + HAConnection.this.slaveRequestOffset);
                    }

                    // lastWriteOver表示上次同步是否完成
                    if (this.lastWriteOver) {

                        long interval =
                            HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now() - this.lastWriteTimestamp;

                        // 上一次同步时间到现在间隔超过5秒，master会发送一个心跳包
                        if (interval > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig()
                            .getHaSendHeartbeatInterval()) {

                            // Build Header
                            this.byteBufferHeader.position(0);
                            this.byteBufferHeader.limit(headerSize);
                            this.byteBufferHeader.putLong(this.nextTransferFromWhere);
                            this.byteBufferHeader.putInt(0);
                            this.byteBufferHeader.flip();

                            this.lastWriteOver = this.transferData();
                            if (!this.lastWriteOver)
                                continue;
                        }
                    }
                    // 说明上次数据没有同步完，继续同步
                    else {
                        this.lastWriteOver = this.transferData();
                        if (!this.lastWriteOver)
                            continue;
                    }

                    // 根据要发送的起始偏移量，从CommitLog文件中读取信息，如果master已经同步了所有数据，则selectResult为null
                    SelectMappedBufferResult selectResult =
                        HAConnection.this.haService.getDefaultMessageStore().getCommitLogData(this.nextTransferFromWhere);
                    if (selectResult != null) {
                        int size = selectResult.getSize();
                        // 默认一次最多同步32K数据
                        if (size > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize()) {
                            size = HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize();
                        }

                        // 记录当前要同步数据的起始偏移量
                        long thisOffset = this.nextTransferFromWhere;

                        // 每同步一次数据后，下次同步的起始偏移量就会发生变化
                        this.nextTransferFromWhere += size;

                        selectResult.getByteBuffer().limit(size);

                        // 每次要同步的数据
                        this.selectMappedBufferResult = selectResult;

                        // Build Header
                        this.byteBufferHeader.position(0);
                        this.byteBufferHeader.limit(headerSize);
                        // 前8个字节放偏移量
                        this.byteBufferHeader.putLong(thisOffset);
                        // 后四个字节放数据大小
                        this.byteBufferHeader.putInt(size);
                        this.byteBufferHeader.flip();

                        // 发送数据
                        this.lastWriteOver = this.transferData();
                    } else {

                        /*
                            master如果已经同步了所有数据，则会进行阻塞，直到100ms超时，
                            或者是如果master是同步类型的，可能会被HAConnection.ReadSocketService.processReadEvent中的
                            salve上报消息的操作唤醒
                         */
                        HAConnection.this.haService.getWaitNotifyObject().allWaitForRunning(100);
                    }
                } catch (Exception e) {

                    HAConnection.log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }

            HAConnection.this.haService.getWaitNotifyObject().removeFromWaitingThreadTable();

            if (this.selectMappedBufferResult != null) {
                this.selectMappedBufferResult.release();
            }

            this.makeStop();

            readSocketService.makeStop();

            haService.removeConnection(HAConnection.this);

            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                this.selector.close();
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }

            HAConnection.log.info(this.getServiceName() + " service end");
        }

        /**
         * master同步数据到slave
         * @return
         * @throws Exception
         */
        private boolean transferData() throws Exception {
            int writeSizeZeroTimes = 0;
            // Write Header
            /*
                byteBufferHeader中存放的是头部数据，前8个字节存放发送数据的起始偏移量，
                后4个字节存放的是数据的大小。
                这里首先将头部数据发送出去
             */
            while (this.byteBufferHeader.hasRemaining()) {
                int writeSize = this.socketChannel.write(this.byteBufferHeader);
                if (writeSize > 0) {
                    writeSizeZeroTimes = 0;
                    this.lastWriteTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                } else if (writeSize == 0) {
                    if (++writeSizeZeroTimes >= 3) {
                        break;
                    }
                } else {
                    throw new Exception("ha master write header error < 0");
                }
            }

            // selectMappedBufferResult如果为null，说明只是心跳包，只发送头部数据
            if (null == this.selectMappedBufferResult) {
                return !this.byteBufferHeader.hasRemaining();
            }

            writeSizeZeroTimes = 0;

            // Write Body
            if (!this.byteBufferHeader.hasRemaining()) {
                // selectMappedBufferResult存放了master向slave要发送的数据，这里进行发送数据到slave的操作
                while (this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
                    int writeSize = this.socketChannel.write(this.selectMappedBufferResult.getByteBuffer());
                    if (writeSize > 0) {
                        writeSizeZeroTimes = 0;
                        this.lastWriteTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                    } else if (writeSize == 0) {
                        if (++writeSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        throw new Exception("ha master write body error < 0");
                    }
                }
            }

            boolean result = !this.byteBufferHeader.hasRemaining() && !this.selectMappedBufferResult.getByteBuffer().hasRemaining();

            if (!this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
                this.selectMappedBufferResult.release();
                this.selectMappedBufferResult = null;
            }

            return result;
        }

        @Override
        public String getServiceName() {
            return WriteSocketService.class.getSimpleName();
        }

        @Override
        public void shutdown() {
            super.shutdown();
        }
    }
}
