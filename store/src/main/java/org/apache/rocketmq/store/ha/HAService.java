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
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.PutMessageSpinLock;
import org.apache.rocketmq.store.PutMessageStatus;

/**
 * 高可用服务，主从同步
 * 实现原理：
 * 1. 主服务器启动，并在特定的端口上监听从服务器的连接
 * 2. 从服务器主动连接主服务器，主服务器接收客户端的连接，并建立TCP连接
 * 3. 从服务器主动向主服务器发送待拉取消息偏移量，主服务器解析请求并返回消息给从服务器
 * 4. 从服务器保存消息并继续发送新的消息同步请求
 */
public class HAService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final AtomicInteger connectionCount = new AtomicInteger(0);

    private final List<HAConnection> connectionList = new LinkedList<>();

    /**
     * master监听slave连接的服务
     */
    private final AcceptSocketService acceptSocketService;

    /**
     * 默认消息存储服务
     */
    private final DefaultMessageStore defaultMessageStore;

    private final WaitNotifyObject waitNotifyObject = new WaitNotifyObject();

    /**
     * 已经向slave同步的最大偏移量
     */
    private final AtomicLong push2SlaveMaxOffset = new AtomicLong(0);

    private final GroupTransferService groupTransferService;

    /**
     * 客户端
     */
    private final HAClient haClient;

    public HAService(final DefaultMessageStore defaultMessageStore) throws IOException {
        this.defaultMessageStore = defaultMessageStore;
        // 实例化接受请求的服务，端口是Broker的端口+1
        this.acceptSocketService =
            new AcceptSocketService(defaultMessageStore.getMessageStoreConfig().getHaListenPort());
        this.groupTransferService = new GroupTransferService();

        // 实例化客户端
        this.haClient = new HAClient();
    }

    public void updateMasterAddress(final String newAddr) {
        if (this.haClient != null) {
            this.haClient.updateMasterAddress(newAddr);
        }
    }

    public void putRequest(final CommitLog.GroupCommitRequest request) {
        this.groupTransferService.putRequest(request);
    }

    public boolean isSlaveOK(final long masterPutWhere) {
        boolean result = this.connectionCount.get() > 0;
        result =
            result
                && ((masterPutWhere - this.push2SlaveMaxOffset.get()) < this.defaultMessageStore
                .getMessageStoreConfig().getHaSlaveFallbehindMax());
        return result;
    }

    /**
     * notifyTransferSome在master是同步类型的节点时才有用，用来进行相应的唤醒操作。
     * 如果是异步的master则没有要求。
     * @param offset
     */
    public void notifyTransferSome(final long offset) {
        // 本次slave上报的偏移量大于上次slave上报的偏移量
        for (long value = this.push2SlaveMaxOffset.get(); offset > value; ) {
            // 尝试更新新的偏移量
            boolean ok = this.push2SlaveMaxOffset.compareAndSet(value, offset);
            if (ok) {
                // 做一些唤醒操作，只有master是同步的才有用
                this.groupTransferService.notifyTransferSome();
                break;
            } else {
                value = this.push2SlaveMaxOffset.get();
            }
        }
    }

    public AtomicInteger getConnectionCount() {
        return connectionCount;
    }

    // public void notifyTransferSome() {
    // this.groupTransferService.notifyTransferSome();
    // }

    /**
     * 启动高可用服务
     * @throws Exception
     */
    public void start() throws Exception {
        // Java NIO的方式，打开Channel、Selector，将Channel注册到Selector上等
        this.acceptSocketService.beginAccept();

        // Java NIO的方式，开始接受处理请求
        this.acceptSocketService.start();
        this.groupTransferService.start();
        this.haClient.start();
    }

    public void addConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.add(conn);
        }
    }

    public void removeConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.remove(conn);
        }
    }

    public void shutdown() {
        this.haClient.shutdown();
        this.acceptSocketService.shutdown(true);
        this.destroyConnections();
        this.groupTransferService.shutdown();
    }

    public void destroyConnections() {
        synchronized (this.connectionList) {
            for (HAConnection c : this.connectionList) {
                c.shutdown();
            }

            this.connectionList.clear();
        }
    }

    public DefaultMessageStore getDefaultMessageStore() {
        return defaultMessageStore;
    }

    public WaitNotifyObject getWaitNotifyObject() {
        return waitNotifyObject;
    }

    public AtomicLong getPush2SlaveMaxOffset() {
        return push2SlaveMaxOffset;
    }

    /**
     * Listens to slave connections to create {@link HAConnection}.
     * master端监听slave连接，只用来处理连接的accept事件
     */
    class AcceptSocketService extends ServiceThread {
        /**
         * 服务监听套接字
         */
        private final SocketAddress socketAddressListen;
        /**
         * 服务端socket通道
         */
        private ServerSocketChannel serverSocketChannel;
        /**
         * 事件选择器
         */
        private Selector selector;

        public AcceptSocketService(final int port) {
            this.socketAddressListen = new InetSocketAddress(port);
        }

        /**
         * Starts listening to slave connections.
         *
         * @throws Exception If fails.
         */
        public void beginAccept() throws Exception {
            // 打开Channel
            this.serverSocketChannel = ServerSocketChannel.open();

            // 打开Selector
            this.selector = RemotingUtil.openSelector();
            this.serverSocketChannel.socket().setReuseAddress(true);

            // 绑定端口
            this.serverSocketChannel.socket().bind(this.socketAddressListen);

            // 设置为非阻塞
            this.serverSocketChannel.configureBlocking(false);

            // Channel注册到Selector上，注册接受连接事件
            this.serverSocketChannel.register(this.selector, SelectionKey.OP_ACCEPT);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void shutdown(final boolean interrupt) {
            super.shutdown(interrupt);
            try {
                this.serverSocketChannel.close();
                this.selector.close();
            } catch (IOException e) {
                log.error("AcceptSocketService shutdown exception", e);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 带超时的select
                    this.selector.select(1000);
                    Set<SelectionKey> selected = this.selector.selectedKeys();

                    if (selected != null) {
                        for (SelectionKey k : selected) {
                            // 只处理accept事件
                            if ((k.readyOps() & SelectionKey.OP_ACCEPT) != 0) {
                                SocketChannel sc = ((ServerSocketChannel) k.channel()).accept();

                                if (sc != null) {
                                    HAService.log.info("HAService receive new connection, "
                                        + sc.socket().getRemoteSocketAddress());

                                    try {
                                        // 收到slave的上报的请求后，将请求封装成HAConnection对象
                                        HAConnection conn = new HAConnection(HAService.this, sc);
                                        // HAConnection对象开启读写线程
                                        conn.start();
                                        // 添加到连接列表缓存起来
                                        HAService.this.addConnection(conn);
                                    } catch (Exception e) {
                                        log.error("new HAConnection exception", e);
                                        sc.close();
                                    }
                                }
                            } else {
                                log.warn("Unexpected ops in select " + k.readyOps());
                            }
                        }

                        selected.clear();
                    }
                } catch (Exception e) {
                    log.error(this.getServiceName() + " service has exception.", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String getServiceName() {
            return AcceptSocketService.class.getSimpleName();
        }
    }

    /**
     * GroupTransferService Service
     * 主从同步通知实现类，是主从同步阻塞实现
     * 如果是同步主从模式，消息发送者将消息写到磁盘后，需要继续等待新数据被传输到从服务器，
     * 从服务器数据的复制是在另外一个线程HAConnection中去拉取，所以消息发送者在这里需要等待数据传输结果，
     */
    class GroupTransferService extends ServiceThread {

        private final WaitNotifyObject notifyTransferObject = new WaitNotifyObject();
        private final PutMessageSpinLock lock = new PutMessageSpinLock();

        /**
         * 存放要同步的请求
         */
        private volatile LinkedList<CommitLog.GroupCommitRequest> requestsWrite = new LinkedList<>();
        private volatile LinkedList<CommitLog.GroupCommitRequest> requestsRead = new LinkedList<>();

        public void putRequest(final CommitLog.GroupCommitRequest request) {
            lock.lock();
            try {
                this.requestsWrite.add(request);
            } finally {
                lock.unlock();
            }
            this.wakeup();
        }

        /**
         * notifyTransferSome在master是同步类型的节点时才有用，用来进行相应的唤醒操作。
         * 如果是异步的master则没有要求。
         */
        public void notifyTransferSome() {
            this.notifyTransferObject.wakeup();
        }

        /**
         * 交换requestsWrite和requestsRead的数据
         */
        private void swapRequests() {
            lock.lock();
            try {
                LinkedList<CommitLog.GroupCommitRequest> tmp = this.requestsWrite;
                this.requestsWrite = this.requestsRead;
                this.requestsRead = tmp;
            } finally {
                lock.unlock();
            }
        }

        /**
         * 等待同步完成
         */
        private void doWaitTransfer() {
            // 已经将requestsWrite的数据交换到了requestsRead中
            if (!this.requestsRead.isEmpty()) {
                for (CommitLog.GroupCommitRequest req : this.requestsRead) {
                    // push2SlaveMaxOffset同步到slave的最大的偏移量，如果比要同步的偏移量大，说明已经同步过
                    boolean transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                    long waitUntilWhen = HAService.this.defaultMessageStore.getSystemClock().now()
                            + HAService.this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout();
                    while (!transferOK && HAService.this.defaultMessageStore.getSystemClock().now() < waitUntilWhen) {
                        this.notifyTransferObject.waitForRunning(1000);
                        transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                    }

                    if (!transferOK) {
                        log.warn("transfer messsage to slave timeout, " + req.getNextOffset());
                    }

                    // 唤醒写消息的线程中的阻塞
                    req.wakeupCustomer(transferOK ? PutMessageStatus.PUT_OK : PutMessageStatus.FLUSH_SLAVE_TIMEOUT);
                }

                this.requestsRead = new LinkedList<>();
            }
        }

        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 被唤醒后会调用onWaitEnd方法，会交换requestsWrite和requestsRead的数据
                    this.waitForRunning(10);

                    // 等待同步完成
                    this.doWaitTransfer();
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }

        @Override
        public String getServiceName() {
            return GroupTransferService.class.getSimpleName();
        }
    }

    /**
     * HA Client端实现类
     */
    class HAClient extends ServiceThread {
        /**
         * socket读缓冲区大小
         */
        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024 * 4;

        /**
         * master地址
         */
        private final AtomicReference<String> masterAddress = new AtomicReference<>();

        /**
         * slave向master发起主从同步的拉取偏移量
         */
        private final ByteBuffer reportOffset = ByteBuffer.allocate(8);
        private SocketChannel socketChannel;
        private Selector selector;

        /**
         * 上一次写入时间戳
         */
        private long lastWriteTimestamp = System.currentTimeMillis();

        /**
         * 当前salve上报给master的偏移量，是当前slave的commitlog的最大偏移量
         */
        private long currentReportedOffset = 0;

        /**
         * 本次已处理读缓冲区的指针
         */
        private int dispatchPosition = 0;

        /**
         * 读缓冲区，4M
         */
        private ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);

        /**
         * 读缓冲区备份
         */
        private ByteBuffer byteBufferBackup = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);

        public HAClient() throws IOException {
            this.selector = RemotingUtil.openSelector();
        }

        public void updateMasterAddress(final String newAddr) {
            String currentAddr = this.masterAddress.get();
            if (currentAddr == null || !currentAddr.equals(newAddr)) {
                this.masterAddress.set(newAddr);
                log.info("update master address, OLD: " + currentAddr + " NEW: " + newAddr);
            }
        }

        /**
         * 判断是否需要向master反馈当前待拉取偏移量
         * master与slave的HA心跳发送间隔为5s
         * @return
         */
        private boolean isTimeToReportOffset() {
            long interval =
                HAService.this.defaultMessageStore.getSystemClock().now() - this.lastWriteTimestamp;
            boolean needHeart = interval > HAService.this.defaultMessageStore.getMessageStoreConfig()
                .getHaSendHeartbeatInterval();

            return needHeart;
        }

        /**
         * 向master上报当前slave的同步进度
         * @param maxOffset
         * @return
         */
        private boolean reportSlaveMaxOffset(final long maxOffset) {
            this.reportOffset.position(0);
            this.reportOffset.limit(8);
            // 上报的数据是偏移量
            this.reportOffset.putLong(maxOffset);
            this.reportOffset.position(0);
            this.reportOffset.limit(8);

            for (int i = 0; i < 3 && this.reportOffset.hasRemaining(); i++) {
                try {
                    this.socketChannel.write(this.reportOffset);
                } catch (IOException e) {
                    log.error(this.getServiceName()
                        + "reportSlaveMaxOffset this.socketChannel.write exception", e);
                    return false;
                }
            }

            lastWriteTimestamp = HAService.this.defaultMessageStore.getSystemClock().now();
            return !this.reportOffset.hasRemaining();
        }

        private void reallocateByteBuffer() {
            int remain = READ_MAX_BUFFER_SIZE - this.dispatchPosition;
            if (remain > 0) {
                this.byteBufferRead.position(this.dispatchPosition);

                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);
                this.byteBufferBackup.put(this.byteBufferRead);
            }

            this.swapByteBuffer();

            this.byteBufferRead.position(remain);
            this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
            this.dispatchPosition = 0;
        }

        private void swapByteBuffer() {
            ByteBuffer tmp = this.byteBufferRead;
            this.byteBufferRead = this.byteBufferBackup;
            this.byteBufferBackup = tmp;
        }

        /**
         * 处理master返回给当前salve的commitlog的数据
         * @return
         */
        private boolean processReadEvent() {
            int readSizeZeroTimes = 0;
            while (this.byteBufferRead.hasRemaining()) {
                try {
                    int readSize = this.socketChannel.read(this.byteBufferRead);
                    if (readSize > 0) {
                        readSizeZeroTimes = 0;
                        // 处理master发送回来的消息
                        boolean result = this.dispatchReadRequest();
                        if (!result) {
                            log.error("HAClient, dispatchReadRequest error");
                            return false;
                        }
                    } else if (readSize == 0) {
                        if (++readSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        log.info("HAClient, processReadEvent read socket < 0");
                        return false;
                    }
                } catch (IOException e) {
                    log.info("HAClient, processReadEvent read socket exception", e);
                    return false;
                }
            }

            return true;
        }

        /**
         * 处理master发送回来的消息
         * @return
         */
        private boolean dispatchReadRequest() {
            // 头部数据，前8个字节是同步消息的起始偏移量，后4个字节是消息的大小
            final int msgHeaderSize = 8 + 4; // phyoffset + size

            while (true) {
                int diff = this.byteBufferRead.position() - this.dispatchPosition;
                if (diff >= msgHeaderSize) {
                    // 头部数据中前8个字节大小是同步消息起始偏移量
                    long masterPhyOffset = this.byteBufferRead.getLong(this.dispatchPosition);

                    // 头部数据中后4个字节是消息的大小
                    int bodySize = this.byteBufferRead.getInt(this.dispatchPosition + 8);

                    // 当前slave上CommitLog的最大的偏移量
                    long slavePhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();

                    if (slavePhyOffset != 0) {
                        if (slavePhyOffset != masterPhyOffset) {
                            log.error("master pushed offset not equal the max phy offset in slave, SLAVE: "
                                + slavePhyOffset + " MASTER: " + masterPhyOffset);
                            return false;
                        }
                    }

                    /*
                        对于从master发送到slave上的心跳包，并没有过多的处理
                     */

                    // 说明不是心跳，有CommitLog数据发送了过来
                    if (diff >= (msgHeaderSize + bodySize)) {
                        byte[] bodyData = byteBufferRead.array();
                        int dataStart = this.dispatchPosition + msgHeaderSize;

                        // 持久化到CommitLog中，并唤醒ReputMessageService服务，让Consumer进行消费
                        HAService.this.defaultMessageStore.appendToCommitLog(
                                masterPhyOffset, bodyData, dataStart, bodySize);

                        this.dispatchPosition += msgHeaderSize + bodySize;

                        // 持久化完成后，向master上报新的偏移量
                        if (!reportSlaveMaxOffsetPlus()) {
                            return false;
                        }

                        continue;
                    }
                }

                if (!this.byteBufferRead.hasRemaining()) {
                    this.reallocateByteBuffer();
                }

                break;
            }

            return true;
        }

        /**
         * 向master上报新的偏移量
         * @return
         */
        private boolean reportSlaveMaxOffsetPlus() {
            boolean result = true;
            // 当前CommitLog的最大偏移量
            long currentPhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();
            // 如果当前CommitLog的最大偏移量比之前上报的偏移量要大，就继续向master上报最新的偏移量
            if (currentPhyOffset > this.currentReportedOffset) {
                this.currentReportedOffset = currentPhyOffset;
                // 上传最新的偏移量
                result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                if (!result) {
                    this.closeMaster();
                    log.error("HAClient, reportSlaveMaxOffset error, " + this.currentReportedOffset);
                }
            }

            return result;
        }

        /**
         * slave服务器连接master服务器
         * @return
         * @throws ClosedChannelException
         */
        private boolean connectMaster() throws ClosedChannelException {
            if (null == socketChannel) {
                String addr = this.masterAddress.get();
                if (addr != null) {

                    SocketAddress socketAddress = RemotingUtil.string2SocketAddress(addr);
                    if (socketAddress != null) {
                        this.socketChannel = RemotingUtil.connect(socketAddress);
                        if (this.socketChannel != null) {
                            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
                        }
                    }
                }

                // 当前salve上报给master的偏移量，是当前slave的commitlog的最大偏移量
                this.currentReportedOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();

                // 最后一次写的时间
                this.lastWriteTimestamp = System.currentTimeMillis();
            }

            return this.socketChannel != null;
        }

        private void closeMaster() {
            if (null != this.socketChannel) {
                try {

                    SelectionKey sk = this.socketChannel.keyFor(this.selector);
                    if (sk != null) {
                        sk.cancel();
                    }

                    this.socketChannel.close();

                    this.socketChannel = null;
                } catch (IOException e) {
                    log.warn("closeMaster exception. ", e);
                }

                this.lastWriteTimestamp = 0;
                this.dispatchPosition = 0;

                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);

                this.byteBufferRead.position(0);
                this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
            }
        }

        /**
         * 连接到master上，上报当前salve的同步进度到master，处理从master返回的commitlog数据
         */
        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 使用Java NIO连接到master上
                    if (this.connectMaster()) {

                        // 固定间隔向master上报一次当前salve的同步进度，默认5s
                        if (this.isTimeToReportOffset()) {
                            boolean result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                            if (!result) {
                                this.closeMaster();
                            }
                        }

                        // 带超时的select，等待master返回数据
                        this.selector.select(1000);

                        // 处理master返回给当前slave的commitlog的数据
                        boolean ok = this.processReadEvent();
                        if (!ok) {
                            this.closeMaster();
                        }

                        // 进度有变化，上报进度到master
                        if (!reportSlaveMaxOffsetPlus()) {
                            continue;
                        }

                        // 从上一次写到现在的间隔时间
                        long interval =
                            HAService.this.getDefaultMessageStore().getSystemClock().now()
                                - this.lastWriteTimestamp;
                        // 超过了配置的时间，关闭连接
                        if (interval > HAService.this.getDefaultMessageStore().getMessageStoreConfig()
                            .getHaHousekeepingInterval()) {
                            log.warn("HAClient, housekeeping, found this connection[" + this.masterAddress
                                + "] expired, " + interval);
                            this.closeMaster();
                            log.warn("HAClient, master not response some time, so close connection");
                        }
                    } else {
                        this.waitForRunning(1000 * 5);
                    }
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                    this.waitForRunning(1000 * 5);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        @Override
        public void shutdown() {
            super.shutdown();
            closeMaster();
        }

        // private void disableWriteFlag() {
        // if (this.socketChannel != null) {
        // SelectionKey sk = this.socketChannel.keyFor(this.selector);
        // if (sk != null) {
        // int ops = sk.interestOps();
        // ops &= ~SelectionKey.OP_WRITE;
        // sk.interestOps(ops);
        // }
        // }
        // }
        // private void enableWriteFlag() {
        // if (this.socketChannel != null) {
        // SelectionKey sk = this.socketChannel.keyFor(this.selector);
        // if (sk != null) {
        // int ops = sk.interestOps();
        // ops |= SelectionKey.OP_WRITE;
        // sk.interestOps(ops);
        // }
        // }
        // }

        @Override
        public String getServiceName() {
            return HAClient.class.getSimpleName();
        }
    }
}
