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

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.ha.HAService;
import org.apache.rocketmq.store.schedule.ScheduleMessageService;

/**
 * Store all metadata downtime for recovery, data protection reliability
 * 对应commitlog文件，用来存储消息，是一个大文件。
 *
 * 消息存储格式：
 * 1. TOTALSIZE，该消息条目总长度，4字节
 * 2. MAGICCODE，魔数，4字节，固定值0xdaa320a7
 * 3. BODYCRC，消息体crc校验码，4字节
 * 4. QUEUEID，消息消费队列ID，4字节
 * 5. FLAG，消息FLAG，RocketMQ不做处理，仅供应用程序使用，4字节
 * 6. QUEUEOFFSET，消息在消息消费队列的偏移量，8字节
 * 7. PHYSICALOFFSET，消息在CommitLog文件中的偏移量，8字节
 * 8. SYSFLAG，消息系统Flag，例如是否压缩、是否是事务消息等，4字节
 * 9. BORNTIMESTAMP，消息生产者调用消息发送API的时间戳，8字节
 * 10. BORNHOST，消息发送者IP、端口号，8字节
 * 11. STORETIMESTAMP，消息存储时间戳，8字节
 * 12. STOREHOSTADDRESS，Broker服务器IP、端口号，8字节
 * 13. RECONSUMETIMES，消息重试次数，4字节
 * 14. Prepared Transaction Offset，事务消息物理偏移量，8字节
 * 15. BodyLength，消息体长度，4字节
 * 16. Body，消息体内容，长度为bodyLength中存储的值
 * 17. TopicLength，主题存储长度，1字节，表示主题名称不能超过255个字符
 * 18. Topic，主题，长度为TopicLength中存储的值
 * 19. PropertiesLength，消息属性长度，2字节，消息属性长度不能超过65536个字符
 * 20. Properties，消息属性，长度为PropertiesLength中存储的值
 */
public class CommitLog {
    // Message's MAGIC CODE daa320a7
    /**
     * 魔数
     */
    public final static int MESSAGE_MAGIC_CODE = -626843481;
    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    // End of file empty MAGIC CODE cbd43194

    /**
     * 空消息的魔数
     */
    protected final static int BLANK_MAGIC_CODE = -875286124;

    /**
     * MappedFile文件列表，持有所有的MappedFile
     */
    protected final MappedFileQueue mappedFileQueue;

    /**
     * 默认消息存储服务commitLog
     */
    protected final DefaultMessageStore defaultMessageStore;

    /**
     * 消息刷盘服务
     * MappedFile落盘有两种方式：
     * 1. 开启了临时存储池，消息会先写到堆外内存，再提交（commit）到文件通道（FileChannel），也就是写入PageCache，之后再刷（flush）到磁盘上。
     * 2. 没开启临时存储池，消息会直接写到内存映射区域（MappedByteBuffer），然后再刷（flush）到磁盘上。
     *
     * flushCommitLogService不管有没有开启临时存储池，都会被调用，是刷到磁盘上的操作
     */
    private final FlushCommitLogService flushCommitLogService;

    //If TransientStorePool enabled, we must flush message to FileChannel at fixed periods
    /**
     * 只有开启了临时存储池的时候，才会使用该服务，将数据从堆外内存提交（commit）到文件通道（FileChannel），
     * 之后再使用flushCommitLogService进行刷盘操作
     */
    private final FlushCommitLogService commitLogService;

    /**
     * 消息追加后的回调函数
     */
    private final AppendMessageCallback appendMessageCallback;

    /**
     * 存储消息的本地线程
     */
    private final ThreadLocal<PutMessageThreadLocal> putMessageThreadLocal;

    /**
     * 存储Topic所在队列的偏移量
     */
    protected HashMap<String/* topic-queueid */, Long/* offset */> topicQueueTable = new HashMap<String, Long>(1024);
    protected volatile long confirmOffset = -1L;

    private volatile long beginTimeInLock = 0;

    /**
     * 存储消息使用的锁
     */
    protected final PutMessageLock putMessageLock;

    public CommitLog(final DefaultMessageStore defaultMessageStore) {
        // MappedFile文件列表，持有所有的MappedFile
        this.mappedFileQueue = new MappedFileQueue(defaultMessageStore.getMessageStoreConfig().getStorePathCommitLog(),
            defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog(), defaultMessageStore.getAllocateMappedFileService());

        // 默认消息存储服务
        this.defaultMessageStore = defaultMessageStore;

        // 同步刷盘
        if (FlushDiskType.SYNC_FLUSH == defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
            // 同步刷盘的消息刷盘服务
            this.flushCommitLogService = new GroupCommitService();
        }
        // 消息异步刷盘
        else {
            // 异步刷盘的消息刷盘服务
            this.flushCommitLogService = new FlushRealTimeService();
        }

        // 只有开启了临时存储池的时候，才会使用该服务，将数据从堆外内存提交（commit）到文件通道（FileChannel）
        this.commitLogService = new CommitRealTimeService();

        // 消息追加后的回调函数
        this.appendMessageCallback = new DefaultAppendMessageCallback(defaultMessageStore.getMessageStoreConfig().getMaxMessageSize());

        // 存储消息的本地线程
        putMessageThreadLocal = new ThreadLocal<PutMessageThreadLocal>() {
            @Override
            protected PutMessageThreadLocal initialValue() {
                return new PutMessageThreadLocal(defaultMessageStore.getMessageStoreConfig().getMaxMessageSize());
            }
        };

        // 存储消息时用的锁可以是可重入锁，也可以是自旋锁
        this.putMessageLock = defaultMessageStore.getMessageStoreConfig().isUseReentrantLockWhenPutMessage() ? new PutMessageReentrantLock() : new PutMessageSpinLock();

    }

    public boolean load() {
        boolean result = this.mappedFileQueue.load();
        log.info("load commit log " + (result ? "OK" : "Failed"));
        return result;
    }

    public void start() {
        this.flushCommitLogService.start();

        if (defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
            this.commitLogService.start();
        }
    }

    public void shutdown() {
        if (defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
            this.commitLogService.shutdown();
        }

        this.flushCommitLogService.shutdown();
    }

    public long flush() {
        this.mappedFileQueue.commit(0);
        this.mappedFileQueue.flush(0);
        return this.mappedFileQueue.getFlushedWhere();
    }

    public long getMaxOffset() {
        return this.mappedFileQueue.getMaxOffset();
    }

    public long remainHowManyDataToCommit() {
        return this.mappedFileQueue.remainHowManyDataToCommit();
    }

    public long remainHowManyDataToFlush() {
        return this.mappedFileQueue.remainHowManyDataToFlush();
    }

    public int deleteExpiredFile(
        final long expiredTime,
        final int deleteFilesInterval,
        final long intervalForcibly,
        final boolean cleanImmediately
    ) {
        return this.mappedFileQueue.deleteExpiredFileByTime(expiredTime, deleteFilesInterval, intervalForcibly, cleanImmediately);
    }

    /**
     * Read CommitLog data, use data replication
     * 从CommitLog文件中读取指定offset后面的数据
     */
    public SelectMappedBufferResult getData(final long offset) {
        return this.getData(offset, offset == 0);
    }

    public SelectMappedBufferResult getData(final long offset, final boolean returnFirstOnNotFound) {
        // commitlog文件大小，默认1G
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        // 根据offset从mappedFileQueue中查找对应的MappedFile
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, returnFirstOnNotFound);
        if (mappedFile != null) {
            // 得到MappedByteBuffer对应的position
            int pos = (int) (offset % mappedFileSize);
            // slice MappedByteBuffer的一部分返回
            SelectMappedBufferResult result = mappedFile.selectMappedBuffer(pos);
            return result;
        }

        return null;
    }

    /**
     * When the normal exit, data recovery, all memory data have been flush
     */
    public void recoverNormally(long maxPhyOffsetOfConsumeQueue) {
        boolean checkCRCOnRecover = this.defaultMessageStore.getMessageStoreConfig().isCheckCRCOnRecover();
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            // Began to recover from the last third file
            int index = mappedFiles.size() - 3;
            if (index < 0)
                index = 0;

            MappedFile mappedFile = mappedFiles.get(index);
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            long processOffset = mappedFile.getFileFromOffset();
            long mappedFileOffset = 0;
            while (true) {
                DispatchRequest dispatchRequest = this.checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover);
                int size = dispatchRequest.getMsgSize();
                // Normal data
                if (dispatchRequest.isSuccess() && size > 0) {
                    mappedFileOffset += size;
                }
                // Come the end of the file, switch to the next file Since the
                // return 0 representatives met last hole,
                // this can not be included in truncate offset
                else if (dispatchRequest.isSuccess() && size == 0) {
                    index++;
                    if (index >= mappedFiles.size()) {
                        // Current branch can not happen
                        log.info("recover last 3 physics file over, last mapped file " + mappedFile.getFileName());
                        break;
                    } else {
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        log.info("recover next physics file, " + mappedFile.getFileName());
                    }
                }
                // Intermediate file read error
                else if (!dispatchRequest.isSuccess()) {
                    log.info("recover physics file end, " + mappedFile.getFileName());
                    break;
                }
            }

            processOffset += mappedFileOffset;
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            this.mappedFileQueue.truncateDirtyFiles(processOffset);

            // Clear ConsumeQueue redundant data
            if (maxPhyOffsetOfConsumeQueue >= processOffset) {
                log.warn("maxPhyOffsetOfConsumeQueue({}) >= processOffset({}), truncate dirty logic files", maxPhyOffsetOfConsumeQueue, processOffset);
                this.defaultMessageStore.truncateDirtyLogicFiles(processOffset);
            }
        } else {
            // Commitlog case files are deleted
            log.warn("The commitlog files are deleted, and delete the consume queue files");
            this.mappedFileQueue.setFlushedWhere(0);
            this.mappedFileQueue.setCommittedWhere(0);
            this.defaultMessageStore.destroyLogics();
        }
    }

    public DispatchRequest checkMessageAndReturnSize(java.nio.ByteBuffer byteBuffer, final boolean checkCRC) {
        return this.checkMessageAndReturnSize(byteBuffer, checkCRC, true);
    }

    private void doNothingForDeadCode(final Object obj) {
        if (obj != null) {
            log.debug(String.valueOf(obj.hashCode()));
        }
    }

    /**
     * check the message and returns the message size
     *
     * @return 0 Come the end of the file // >0 Normal messages // -1 Message checksum failure
     */
    public DispatchRequest checkMessageAndReturnSize(java.nio.ByteBuffer byteBuffer, final boolean checkCRC,
        final boolean readBody) {
        try {
            // 1 TOTAL SIZE
            int totalSize = byteBuffer.getInt();

            // 2 MAGIC CODE
            int magicCode = byteBuffer.getInt();
            switch (magicCode) {
                case MESSAGE_MAGIC_CODE:
                    break;
                case BLANK_MAGIC_CODE:
                    return new DispatchRequest(0, true /* success */);
                default:
                    log.warn("found a illegal magic code 0x" + Integer.toHexString(magicCode));
                    return new DispatchRequest(-1, false /* success */);
            }

            byte[] bytesContent = new byte[totalSize];

            int bodyCRC = byteBuffer.getInt();

            int queueId = byteBuffer.getInt();

            int flag = byteBuffer.getInt();

            long queueOffset = byteBuffer.getLong();

            long physicOffset = byteBuffer.getLong();

            int sysFlag = byteBuffer.getInt();

            long bornTimeStamp = byteBuffer.getLong();

            ByteBuffer byteBuffer1;
            if ((sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0) {
                byteBuffer1 = byteBuffer.get(bytesContent, 0, 4 + 4);
            } else {
                byteBuffer1 = byteBuffer.get(bytesContent, 0, 16 + 4);
            }

            long storeTimestamp = byteBuffer.getLong();

            ByteBuffer byteBuffer2;
            if ((sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0) {
                byteBuffer2 = byteBuffer.get(bytesContent, 0, 4 + 4);
            } else {
                byteBuffer2 = byteBuffer.get(bytesContent, 0, 16 + 4);
            }

            int reconsumeTimes = byteBuffer.getInt();

            long preparedTransactionOffset = byteBuffer.getLong();

            int bodyLen = byteBuffer.getInt();
            if (bodyLen > 0) {
                if (readBody) {
                    byteBuffer.get(bytesContent, 0, bodyLen);

                    if (checkCRC) {
                        int crc = UtilAll.crc32(bytesContent, 0, bodyLen);
                        if (crc != bodyCRC) {
                            log.warn("CRC check failed. bodyCRC={}, currentCRC={}", crc, bodyCRC);
                            return new DispatchRequest(-1, false/* success */);
                        }
                    }
                } else {
                    byteBuffer.position(byteBuffer.position() + bodyLen);
                }
            }

            byte topicLen = byteBuffer.get();
            byteBuffer.get(bytesContent, 0, topicLen);
            String topic = new String(bytesContent, 0, topicLen, MessageDecoder.CHARSET_UTF8);

            long tagsCode = 0;
            String keys = "";
            String uniqKey = null;

            short propertiesLength = byteBuffer.getShort();
            Map<String, String> propertiesMap = null;
            if (propertiesLength > 0) {
                byteBuffer.get(bytesContent, 0, propertiesLength);
                String properties = new String(bytesContent, 0, propertiesLength, MessageDecoder.CHARSET_UTF8);
                propertiesMap = MessageDecoder.string2messageProperties(properties);

                keys = propertiesMap.get(MessageConst.PROPERTY_KEYS);

                uniqKey = propertiesMap.get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);

                String tags = propertiesMap.get(MessageConst.PROPERTY_TAGS);
                if (tags != null && tags.length() > 0) {
                    tagsCode = MessageExtBrokerInner.tagsString2tagsCode(MessageExt.parseTopicFilterType(sysFlag), tags);
                }

                // Timing message processing
                {
                    String t = propertiesMap.get(MessageConst.PROPERTY_DELAY_TIME_LEVEL);
                    if (TopicValidator.RMQ_SYS_SCHEDULE_TOPIC.equals(topic) && t != null) {
                        int delayLevel = Integer.parseInt(t);

                        if (delayLevel > this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel()) {
                            delayLevel = this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel();
                        }

                        if (delayLevel > 0) {
                            tagsCode = this.defaultMessageStore.getScheduleMessageService().computeDeliverTimestamp(delayLevel,
                                storeTimestamp);
                        }
                    }
                }
            }

            int readLength = calMsgLength(sysFlag, bodyLen, topicLen, propertiesLength);
            if (totalSize != readLength) {
                doNothingForDeadCode(reconsumeTimes);
                doNothingForDeadCode(flag);
                doNothingForDeadCode(bornTimeStamp);
                doNothingForDeadCode(byteBuffer1);
                doNothingForDeadCode(byteBuffer2);
                log.error(
                    "[BUG]read total count not equals msg total size. totalSize={}, readTotalCount={}, bodyLen={}, topicLen={}, propertiesLength={}",
                    totalSize, readLength, bodyLen, topicLen, propertiesLength);
                return new DispatchRequest(totalSize, false/* success */);
            }

            return new DispatchRequest(
                topic,
                queueId,
                physicOffset,
                totalSize,
                tagsCode,
                storeTimestamp,
                queueOffset,
                keys,
                uniqKey,
                sysFlag,
                preparedTransactionOffset,
                propertiesMap
            );
        } catch (Exception e) {
        }

        return new DispatchRequest(-1, false /* success */);
    }

    protected static int calMsgLength(int sysFlag, int bodyLength, int topicLength, int propertiesLength) {
        int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
        int storehostAddressLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 8 : 20;
        final int msgLen = 4 //TOTALSIZE
            + 4 //MAGICCODE
            + 4 //BODYCRC
            + 4 //QUEUEID
            + 4 //FLAG
            + 8 //QUEUEOFFSET
            + 8 //PHYSICALOFFSET
            + 4 //SYSFLAG
            + 8 //BORNTIMESTAMP
            + bornhostLength //BORNHOST
            + 8 //STORETIMESTAMP
            + storehostAddressLength //STOREHOSTADDRESS
            + 4 //RECONSUMETIMES
            + 8 //Prepared Transaction Offset
            + 4 + (bodyLength > 0 ? bodyLength : 0) //BODY
            + 1 + topicLength //TOPIC
            + 2 + (propertiesLength > 0 ? propertiesLength : 0) //propertiesLength
            + 0;
        return msgLen;
    }

    public long getConfirmOffset() {
        return this.confirmOffset;
    }

    public void setConfirmOffset(long phyOffset) {
        this.confirmOffset = phyOffset;
    }

    @Deprecated
    public void recoverAbnormally(long maxPhyOffsetOfConsumeQueue) {
        // recover by the minimum time stamp
        boolean checkCRCOnRecover = this.defaultMessageStore.getMessageStoreConfig().isCheckCRCOnRecover();
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            // Looking beginning to recover from which file
            int index = mappedFiles.size() - 1;
            MappedFile mappedFile = null;
            for (; index >= 0; index--) {
                mappedFile = mappedFiles.get(index);
                if (this.isMappedFileMatchedRecover(mappedFile)) {
                    log.info("recover from this mapped file " + mappedFile.getFileName());
                    break;
                }
            }

            if (index < 0) {
                index = 0;
                mappedFile = mappedFiles.get(index);
            }

            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            long processOffset = mappedFile.getFileFromOffset();
            long mappedFileOffset = 0;
            while (true) {
                DispatchRequest dispatchRequest = this.checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover);
                int size = dispatchRequest.getMsgSize();

                if (dispatchRequest.isSuccess()) {
                    // Normal data
                    if (size > 0) {
                        mappedFileOffset += size;

                        if (this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
                            if (dispatchRequest.getCommitLogOffset() < this.defaultMessageStore.getConfirmOffset()) {
                                this.defaultMessageStore.doDispatch(dispatchRequest);
                            }
                        } else {
                            this.defaultMessageStore.doDispatch(dispatchRequest);
                        }
                    }
                    // Come the end of the file, switch to the next file
                    // Since the return 0 representatives met last hole, this can
                    // not be included in truncate offset
                    else if (size == 0) {
                        index++;
                        if (index >= mappedFiles.size()) {
                            // The current branch under normal circumstances should
                            // not happen
                            log.info("recover physics file over, last mapped file " + mappedFile.getFileName());
                            break;
                        } else {
                            mappedFile = mappedFiles.get(index);
                            byteBuffer = mappedFile.sliceByteBuffer();
                            processOffset = mappedFile.getFileFromOffset();
                            mappedFileOffset = 0;
                            log.info("recover next physics file, " + mappedFile.getFileName());
                        }
                    }
                } else {
                    log.info("recover physics file end, " + mappedFile.getFileName() + " pos=" + byteBuffer.position());
                    break;
                }
            }

            processOffset += mappedFileOffset;
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            this.mappedFileQueue.truncateDirtyFiles(processOffset);

            // Clear ConsumeQueue redundant data
            if (maxPhyOffsetOfConsumeQueue >= processOffset) {
                log.warn("maxPhyOffsetOfConsumeQueue({}) >= processOffset({}), truncate dirty logic files", maxPhyOffsetOfConsumeQueue, processOffset);
                this.defaultMessageStore.truncateDirtyLogicFiles(processOffset);
            }
        }
        // Commitlog case files are deleted
        else {
            log.warn("The commitlog files are deleted, and delete the consume queue files");
            this.mappedFileQueue.setFlushedWhere(0);
            this.mappedFileQueue.setCommittedWhere(0);
            this.defaultMessageStore.destroyLogics();
        }
    }

    private boolean isMappedFileMatchedRecover(final MappedFile mappedFile) {
        ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();

        int magicCode = byteBuffer.getInt(MessageDecoder.MESSAGE_MAGIC_CODE_POSTION);
        if (magicCode != MESSAGE_MAGIC_CODE) {
            return false;
        }

        int sysFlag = byteBuffer.getInt(MessageDecoder.SYSFLAG_POSITION);
        int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
        int msgStoreTimePos = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + bornhostLength;
        long storeTimestamp = byteBuffer.getLong(msgStoreTimePos);
        if (0 == storeTimestamp) {
            return false;
        }

        if (this.defaultMessageStore.getMessageStoreConfig().isMessageIndexEnable()
            && this.defaultMessageStore.getMessageStoreConfig().isMessageIndexSafe()) {
            if (storeTimestamp <= this.defaultMessageStore.getStoreCheckpoint().getMinTimestampIndex()) {
                log.info("find check timestamp, {} {}",
                    storeTimestamp,
                    UtilAll.timeMillisToHumanString(storeTimestamp));
                return true;
            }
        } else {
            if (storeTimestamp <= this.defaultMessageStore.getStoreCheckpoint().getMinTimestamp()) {
                log.info("find check timestamp, {} {}",
                    storeTimestamp,
                    UtilAll.timeMillisToHumanString(storeTimestamp));
                return true;
            }
        }

        return false;
    }

    private void notifyMessageArriving() {

    }

    public boolean resetOffset(long offset) {
        return this.mappedFileQueue.resetOffset(offset);
    }

    public long getBeginTimeInLock() {
        return beginTimeInLock;
    }

    private String generateKey(StringBuilder keyBuilder, MessageExt messageExt) {
        keyBuilder.setLength(0);
        keyBuilder.append(messageExt.getTopic());
        keyBuilder.append('-');
        keyBuilder.append(messageExt.getQueueId());
        return keyBuilder.toString();
    }

    /**
     * 异步存储消息
     * @param msg
     * @return
     */
    public CompletableFuture<PutMessageResult> asyncPutMessage(final MessageExtBrokerInner msg) {
        // Set the storage time
        msg.setStoreTimestamp(System.currentTimeMillis());
        // Set the message body BODY CRC (consider the most appropriate setting
        // on the client)
        msg.setBodyCRC(UtilAll.crc32(msg.getBody()));
        // Back to Results
        AppendMessageResult result = null;

        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

        String topic = msg.getTopic();
        int queueId = msg.getQueueId();

        // 事务消息
        final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());
        if (tranType == MessageSysFlag.TRANSACTION_NOT_TYPE
                || tranType == MessageSysFlag.TRANSACTION_COMMIT_TYPE) {
            // Delay Delivery
            // 延迟发送的消息
            if (msg.getDelayTimeLevel() > 0) {
                if (msg.getDelayTimeLevel() > this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel()) {
                    msg.setDelayTimeLevel(this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel());
                }

                // 延迟消息topic为SCHEDULE_TOPIC_XXXX
                topic = TopicValidator.RMQ_SYS_SCHEDULE_TOPIC;
                // 延迟消息queueId = delayLevel - 1
                queueId = ScheduleMessageService.delayLevel2QueueId(msg.getDelayTimeLevel());

                // Backup real topic, queueId
                // 将原Topic和QueueId存入消息属性中
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_TOPIC, msg.getTopic());
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_QUEUE_ID, String.valueOf(msg.getQueueId()));
                msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));

                msg.setTopic(topic);
                msg.setQueueId(queueId);
            }
        }

        InetSocketAddress bornSocketAddress = (InetSocketAddress) msg.getBornHost();
        if (bornSocketAddress.getAddress() instanceof Inet6Address) {
            msg.setBornHostV6Flag();
        }

        InetSocketAddress storeSocketAddress = (InetSocketAddress) msg.getStoreHost();
        if (storeSocketAddress.getAddress() instanceof Inet6Address) {
            msg.setStoreHostAddressV6Flag();
        }

        PutMessageThreadLocal putMessageThreadLocal = this.putMessageThreadLocal.get();
        PutMessageResult encodeResult = putMessageThreadLocal.getEncoder().encode(msg);
        if (encodeResult != null) {
            return CompletableFuture.completedFuture(encodeResult);
        }
        msg.setEncodedBuff(putMessageThreadLocal.getEncoder().encoderBuffer);
        PutMessageContext putMessageContext = new PutMessageContext(generateKey(putMessageThreadLocal.getKeyBuilder(), msg));

        /**
         * 下面是获取可写入的CommitLog文件
         *
         * CommitLog文件存储目录为${ROCKET_HOME}/store/commitlog目录下，
         * 每个文件默认1G，一个文件写满后再创建另外一个，以该文件中第一
         * 个偏移量作为文件名，偏移量小于20位用0补齐
         */
        long elapsedTimeInLock = 0;
        MappedFile unlockMappedFile = null;

        // 先申请锁，消息存储到CommitLog文件中是串行的
        putMessageLock.lock(); //spin or ReentrantLock ,depending on store config
        try {
            // 从mappedFileQueue中的mappedFiles集合中获取最后一个MappedFile
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
            // 设置消息存储时间
            long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
            this.beginTimeInLock = beginLockTimestamp;

            // Here settings are stored timestamp, in order to ensure an orderly
            // global
            msg.setStoreTimestamp(beginLockTimestamp);

            // mappedFile为空，说明commitlog目录下没有文件，第一次消息发送，需要用偏移量0创建第一个文件
            if (null == mappedFile || mappedFile.isFull()) {
                mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
            }
            if (null == mappedFile) {
                log.error("create mapped file1 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                beginTimeInLock = 0;
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, null));
            }

            // 往commitlog文件中写数据
            result = mappedFile.appendMessage(msg, this.appendMessageCallback, putMessageContext);
            switch (result.getStatus()) {
                case PUT_OK:
                    break;
                case END_OF_FILE:
                    // 文件末尾，获取新文件
                    unlockMappedFile = mappedFile;
                    // Create a new file, re-write the message
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                    if (null == mappedFile) {
                        // XXX: warn and notify me
                        log.error("create mapped file2 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                        beginTimeInLock = 0;
                        return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, result));
                    }
                    // 往MappedFile中追加消息
                    result = mappedFile.appendMessage(msg, this.appendMessageCallback, putMessageContext);
                    break;
                case MESSAGE_SIZE_EXCEEDED:
                case PROPERTIES_SIZE_EXCEEDED:
                    beginTimeInLock = 0;
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result));
                case UNKNOWN_ERROR:
                    beginTimeInLock = 0;
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
                default:
                    beginTimeInLock = 0;
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
            }

            elapsedTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
            beginTimeInLock = 0;
        } finally {
            putMessageLock.unlock();
        }

        if (elapsedTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessage in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", elapsedTimeInLock, msg.getBody().length, result);
        }

        if (null != unlockMappedFile && this.defaultMessageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
            this.defaultMessageStore.unlockMappedFile(unlockMappedFile);
        }

        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // Statistics
        // 统计
        storeStatsService.getSinglePutMessageTopicTimesTotal(msg.getTopic()).incrementAndGet();
        storeStatsService.getSinglePutMessageTopicSizeTotal(topic).addAndGet(result.getWroteBytes());

        // 提交刷盘请求
        CompletableFuture<PutMessageStatus> flushResultFuture = submitFlushRequest(result, msg);

        // 提交主从同步复制请求，同步双写
        CompletableFuture<PutMessageStatus> replicaResultFuture = submitReplicaRequest(result, msg);
        return flushResultFuture.thenCombine(replicaResultFuture, (flushStatus, replicaStatus) -> {
            if (flushStatus != PutMessageStatus.PUT_OK) {
                putMessageResult.setPutMessageStatus(flushStatus);
            }
            if (replicaStatus != PutMessageStatus.PUT_OK) {
                putMessageResult.setPutMessageStatus(replicaStatus);
                if (replicaStatus == PutMessageStatus.FLUSH_SLAVE_TIMEOUT) {
                    log.error("do sync transfer other node, wait return, but failed, topic: {} tags: {} client address: {}",
                            msg.getTopic(), msg.getTags(), msg.getBornHostNameString());
                }
            }
            return putMessageResult;
        });
    }

    public CompletableFuture<PutMessageResult> asyncPutMessages(final MessageExtBatch messageExtBatch) {
        messageExtBatch.setStoreTimestamp(System.currentTimeMillis());
        AppendMessageResult result;

        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

        final int tranType = MessageSysFlag.getTransactionValue(messageExtBatch.getSysFlag());

        if (tranType != MessageSysFlag.TRANSACTION_NOT_TYPE) {
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
        }
        if (messageExtBatch.getDelayTimeLevel() > 0) {
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
        }

        InetSocketAddress bornSocketAddress = (InetSocketAddress) messageExtBatch.getBornHost();
        if (bornSocketAddress.getAddress() instanceof Inet6Address) {
            messageExtBatch.setBornHostV6Flag();
        }

        InetSocketAddress storeSocketAddress = (InetSocketAddress) messageExtBatch.getStoreHost();
        if (storeSocketAddress.getAddress() instanceof Inet6Address) {
            messageExtBatch.setStoreHostAddressV6Flag();
        }

        long elapsedTimeInLock = 0;
        MappedFile unlockMappedFile = null;
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

        //fine-grained lock instead of the coarse-grained
        PutMessageThreadLocal pmThreadLocal = this.putMessageThreadLocal.get();
        MessageExtEncoder batchEncoder = pmThreadLocal.getEncoder();

        PutMessageContext putMessageContext = new PutMessageContext(generateKey(pmThreadLocal.getKeyBuilder(), messageExtBatch));
        messageExtBatch.setEncodedBuff(batchEncoder.encode(messageExtBatch, putMessageContext));

        putMessageLock.lock();
        try {
            long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
            this.beginTimeInLock = beginLockTimestamp;

            // Here settings are stored timestamp, in order to ensure an orderly
            // global
            messageExtBatch.setStoreTimestamp(beginLockTimestamp);

            if (null == mappedFile || mappedFile.isFull()) {
                mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
            }
            if (null == mappedFile) {
                log.error("Create mapped file1 error, topic: {} clientAddr: {}", messageExtBatch.getTopic(), messageExtBatch.getBornHostString());
                beginTimeInLock = 0;
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, null));
            }

            result = mappedFile.appendMessages(messageExtBatch, this.appendMessageCallback, putMessageContext);
            switch (result.getStatus()) {
                case PUT_OK:
                    break;
                case END_OF_FILE:
                    unlockMappedFile = mappedFile;
                    // Create a new file, re-write the message
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                    if (null == mappedFile) {
                        // XXX: warn and notify me
                        log.error("Create mapped file2 error, topic: {} clientAddr: {}", messageExtBatch.getTopic(), messageExtBatch.getBornHostString());
                        beginTimeInLock = 0;
                        return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, result));
                    }
                    result = mappedFile.appendMessages(messageExtBatch, this.appendMessageCallback, putMessageContext);
                    break;
                case MESSAGE_SIZE_EXCEEDED:
                case PROPERTIES_SIZE_EXCEEDED:
                    beginTimeInLock = 0;
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result));
                case UNKNOWN_ERROR:
                default:
                    beginTimeInLock = 0;
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
            }

            elapsedTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
            beginTimeInLock = 0;
        } finally {
            putMessageLock.unlock();
        }

        if (elapsedTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessages in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", elapsedTimeInLock, messageExtBatch.getBody().length, result);
        }

        if (null != unlockMappedFile && this.defaultMessageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
            this.defaultMessageStore.unlockMappedFile(unlockMappedFile);
        }

        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // Statistics
        storeStatsService.getSinglePutMessageTopicTimesTotal(messageExtBatch.getTopic()).addAndGet(result.getMsgNum());
        storeStatsService.getSinglePutMessageTopicSizeTotal(messageExtBatch.getTopic()).addAndGet(result.getWroteBytes());

        CompletableFuture<PutMessageStatus> flushOKFuture = submitFlushRequest(result, messageExtBatch);
        CompletableFuture<PutMessageStatus> replicaOKFuture = submitReplicaRequest(result, messageExtBatch);
        return flushOKFuture.thenCombine(replicaOKFuture, (flushStatus, replicaStatus) -> {
            if (flushStatus != PutMessageStatus.PUT_OK) {
                putMessageResult.setPutMessageStatus(flushStatus);
            }
            if (replicaStatus != PutMessageStatus.PUT_OK) {
                putMessageResult.setPutMessageStatus(replicaStatus);
                if (replicaStatus == PutMessageStatus.FLUSH_SLAVE_TIMEOUT) {
                    log.error("do sync transfer other node, wait return, but failed, topic: {} client address: {}",
                            messageExtBatch.getTopic(), messageExtBatch.getBornHostNameString());
                }
            }
            return putMessageResult;
        });

    }

    /**
     * 提交刷盘请求
     * @param result
     * @param messageExt
     * @return
     */
    public CompletableFuture<PutMessageStatus> submitFlushRequest(AppendMessageResult result, MessageExt messageExt) {
        // Synchronization flush
        // 同步刷盘
        if (FlushDiskType.SYNC_FLUSH == this.defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
            // 同步刷盘服务
            final GroupCommitService service = (GroupCommitService) this.flushCommitLogService;
            if (messageExt.isWaitStoreMsgOK()) {
                // 封装成刷盘请求，提交到GroupCommitService同步刷盘服务的requestsWrite列表中
                GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes(),
                        this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout());
                // 将刷盘请求提交给同步刷盘服务，并唤醒刷盘服务线程进行刷盘
                service.putRequest(request);
                return request.future();
            } else {
                service.wakeup();
                return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
            }
        }
        // Asynchronous flush
        // 异步刷盘
        else {
            /*
                异步刷盘的操作是在一个单独的线程中每隔500ms做一次，每一次刷盘完成后就会暂停500ms，等待
                500ms之后继续刷盘。如果在等待过程中有消息写入了commitlog，此时可以直接调用wakeup方法
                唤醒阻塞中的刷盘线程，让其继续看是不是进行刷盘操作。
             */
            if (!this.defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
                flushCommitLogService.wakeup();
            } else  {
                commitLogService.wakeup();
            }
            return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
        }
    }

    /**
     * 提交主从同步复制请求，同步双写
     * @param result
     * @param messageExt
     * @return
     */
    public CompletableFuture<PutMessageStatus> submitReplicaRequest(AppendMessageResult result, MessageExt messageExt) {
        // master是同步类型的
        if (BrokerRole.SYNC_MASTER == this.defaultMessageStore.getMessageStoreConfig().getBrokerRole()) {
            // 高可用服务
            HAService service = this.defaultMessageStore.getHaService();
            if (messageExt.isWaitStoreMsgOK()) {
                // result.getWroteBytes() + result.getWroteOffset()为写玩消息后新的偏移量
                if (service.isSlaveOK(result.getWroteBytes() + result.getWroteOffset())) {
                    // 创建GroupCommitRequest，放到requestsWrite列表中
                    GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes(),
                            this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout());

                    // 将同步请求放到requestsWrite中，并唤醒GroupTransferService线程
                    service.putRequest(request);
                    // 将所有阻塞的WriteSocketService线程唤醒
                    service.getWaitNotifyObject().wakeupAll();
                    return request.future();
                }
                else {
                    return CompletableFuture.completedFuture(PutMessageStatus.SLAVE_NOT_AVAILABLE);
                }
            }
        }
        // master如果是异步类型的，直接返回，不关心复制的过程
        return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
    }

    /**
     * According to receive certain message or offset storage time if an error occurs, it returns -1
     */
    public long pickupStoreTimestamp(final long offset, final int size) {
        if (offset >= this.getMinOffset()) {
            SelectMappedBufferResult result = this.getMessage(offset, size);
            if (null != result) {
                try {
                    int sysFlag = result.getByteBuffer().getInt(MessageDecoder.SYSFLAG_POSITION);
                    int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
                    int msgStoreTimePos = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + bornhostLength;
                    return result.getByteBuffer().getLong(msgStoreTimePos);
                } finally {
                    result.release();
                }
            }
        }

        return -1;
    }

    /**
     * 获取CommitLog文件的最小偏移量
     * @return
     */
    public long getMinOffset() {
        // 获取目录下的第一个CommitLog文件
        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        // 文件存在
        if (mappedFile != null) {
            // 文件可用
            if (mappedFile.isAvailable()) {
                // 返回文件的起始偏移量
                return mappedFile.getFileFromOffset();
            } else {
                // 返回下一个文件的起始偏移量
                return this.rollNextFile(mappedFile.getFileFromOffset());
            }
        }

        return -1;
    }

    /**
     * 根据偏移量和消息长度查找消息
     * @param offset
     * @param size
     * @return
     */
    public SelectMappedBufferResult getMessage(final long offset, final int size) {
        // 获取一个文件的大小
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        // 根据偏移量找到所在的物理偏移量
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, offset == 0);
        if (mappedFile != null) {
            // 使用offset和文件长度取余，得到在文件内的偏移量
            int pos = (int) (offset % mappedFileSize);
            // 从指定位置读取指定长度的内容返回
            return mappedFile.selectMappedBuffer(pos, size);
        }
        return null;
    }

    /**
     * 根据offset，返回下一个文件的起始偏移量
     * @param offset
     * @return
     */
    public long rollNextFile(final long offset) {
        // 获取文件的大小
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        // 下一个文件的起始偏移量
        return offset + mappedFileSize - offset % mappedFileSize;
    }

    public HashMap<String, Long> getTopicQueueTable() {
        return topicQueueTable;
    }

    public void setTopicQueueTable(HashMap<String, Long> topicQueueTable) {
        this.topicQueueTable = topicQueueTable;
    }

    public void destroy() {
        this.mappedFileQueue.destroy();
    }

    public boolean appendData(long startOffset, byte[] data, int dataStart, int dataLength) {
        putMessageLock.lock();
        try {
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(startOffset);
            if (null == mappedFile) {
                log.error("appendData getLastMappedFile error  " + startOffset);
                return false;
            }

            return mappedFile.appendMessage(data, dataStart, dataLength);
        } finally {
            putMessageLock.unlock();
        }
    }

    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        return this.mappedFileQueue.retryDeleteFirstFile(intervalForcibly);
    }

    public void removeQueueFromTopicQueueTable(final String topic, final int queueId) {
        String key = topic + "-" + queueId;
        synchronized (this) {
            this.topicQueueTable.remove(key);
        }

        log.info("removeQueueFromTopicQueueTable OK Topic: {} QueueId: {}", topic, queueId);
    }

    public void checkSelf() {
        mappedFileQueue.checkSelf();
    }

    public long lockTimeMills() {
        long diff = 0;
        long begin = this.beginTimeInLock;
        if (begin > 0) {
            diff = this.defaultMessageStore.now() - begin;
        }

        if (diff < 0) {
            diff = 0;
        }

        return diff;
    }

    /**
     * 消息刷盘服务
     * MappedFile落盘有两种方式：
     * 1. 开启了临时存储池，消息会先写到堆外内存，再提交（commit）到文件通道（FileChannel），也就是写入PageCache，之后再刷（flush）到磁盘上。
     * 2. 没开启临时存储池，消息会直接写到内存映射区域（MappedByteBuffer），然后再刷（flush）到磁盘上。
     */
    abstract class FlushCommitLogService extends ServiceThread {
        protected static final int RETRY_TIMES_OVER = 10;
    }

    /**
     * 消息提交服务
     * 在开启了临时存储池的时候会使用，开启了临时存储池，消息会先写到堆外内存，再提交（commit）到文件通道（FileChannel），
     * 也就是写入PageCache，之后再刷（flush）到磁盘上。
     *
     * CommitRealTimeService就是commit这一步
     */
    class CommitRealTimeService extends FlushCommitLogService {

        private long lastCommitTimestamp = 0;

        @Override
        public String getServiceName() {
            return CommitRealTimeService.class.getSimpleName();
        }

        @Override
        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");
            while (!this.isStopped()) {
                // 提交消息到ComitLog对应的文件通道的间隔时间，默认为200毫秒
                int interval = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitIntervalCommitLog();

                //  每次commitlog提交时最小的发生变化的页数，默认4页
                int commitDataLeastPages = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitCommitLogLeastPages();

                // 消息完全提交的时间间隔，默认200ms
                int commitDataThoroughInterval =
                    CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitCommitLogThoroughInterval();

                long begin = System.currentTimeMillis();
                // 上一次消息完全提交到现在已经超过了200ms
                if (begin >= (this.lastCommitTimestamp + commitDataThoroughInterval)) {
                    this.lastCommitTimestamp = begin;
                    // commitDataLeastPages置为0
                    commitDataLeastPages = 0;
                }

                try {
                    // 将数据从堆外内存commit到FileChannel中
                    boolean result = CommitLog.this.mappedFileQueue.commit(commitDataLeastPages);
                    long end = System.currentTimeMillis();
                    if (!result) {
                        this.lastCommitTimestamp = end; // result = false means some data committed.
                        //now wake up flush thread.
                        flushCommitLogService.wakeup();
                    }

                    if (end - begin > 500) {
                        log.info("Commit data to file costs {} ms", end - begin);
                    }
                    this.waitForRunning(interval);
                } catch (Throwable e) {
                    CommitLog.log.error(this.getServiceName() + " service has exception. ", e);
                }
            }

            boolean result = false;
            for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
                result = CommitLog.this.mappedFileQueue.commit(0);
                CommitLog.log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
            }
            CommitLog.log.info(this.getServiceName() + " service end");
        }
    }

    /**
     * 异步刷盘服务
     */
    class FlushRealTimeService extends FlushCommitLogService {
        /**
         * 最后flush时间
         */
        private long lastFlushTimestamp = 0;
        private long printTimes = 0;

        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                // 是否定时刷盘
                boolean flushCommitLogTimed = CommitLog.this.defaultMessageStore.getMessageStoreConfig().isFlushCommitLogTimed();

                // 刷盘时间间隔，默认500ms
                int interval = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushIntervalCommitLog();

                // 每次commitlog刷盘时最小的发生变化的页数，默认4页
                int flushPhysicQueueLeastPages = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushCommitLogLeastPages();

                // 刷盘时间间隔，默认10s
                int flushPhysicQueueThoroughInterval =
                    CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushCommitLogThoroughInterval();

                boolean printFlushProgress = false;

                // Print flush progress
                // 从上一次flush的时间到当前时间之间的间隔如果大于了flushPhysicQueueThoroughInterval指定时间，也需要刷盘
                long currentTimeMillis = System.currentTimeMillis();
                if (currentTimeMillis >= (this.lastFlushTimestamp + flushPhysicQueueThoroughInterval)) {
                    this.lastFlushTimestamp = currentTimeMillis;
                    // flushPhysicQueueLeastPages置为0
                    flushPhysicQueueLeastPages = 0;
                    printFlushProgress = (printTimes++ % 10) == 0;
                }

                try {
                    // 如果是定时刷盘，则sleep等待指定的时间
                    if (flushCommitLogTimed) {
                        Thread.sleep(interval);
                    }
                    /*
                        waitForRunning的意思是让线程阻塞一段时间，到了时间后继续运行，或者调用了wakeup进行唤醒。
                        也就是说一次异步刷盘操作后会暂停500ms后在进行刷盘，或者是在暂停的时候被wakeup唤醒了，也可
                        以继续进行刷盘。

                        在有新消息写到commitlog中的时候，会调用wakeup方法唤醒当前刷盘线程。
                     */
                    else {
                        this.waitForRunning(interval);
                    }

                    if (printFlushProgress) {
                        this.printFlushProgress();
                    }

                    long begin = System.currentTimeMillis();
                    // 刷盘
                    CommitLog.this.mappedFileQueue.flush(flushPhysicQueueLeastPages);
                    long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
                    if (storeTimestamp > 0) {
                        CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                    }
                    long past = System.currentTimeMillis() - begin;
                    if (past > 500) {
                        log.info("Flush data to disk costs {} ms", past);
                    }
                } catch (Throwable e) {
                    CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                    this.printFlushProgress();
                }
            }

            // Normal shutdown, to ensure that all the flush before exit
            // broker关闭时，强制flush
            boolean result = false;
            for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
                result = CommitLog.this.mappedFileQueue.flush(0);
                CommitLog.log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
            }

            this.printFlushProgress();

            CommitLog.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return FlushRealTimeService.class.getSimpleName();
        }

        private void printFlushProgress() {
            // CommitLog.log.info("how much disk fall behind memory, "
            // + CommitLog.this.mappedFileQueue.howMuchFallBehind());
        }

        @Override
        public long getJointime() {
            return 1000 * 60 * 5;
        }
    }

    /**
     * 同步刷盘请求对象
     */
    public static class GroupCommitRequest {

        /**
         * 要进行刷盘的数据的偏移量
         */
        private final long nextOffset;
        private CompletableFuture<PutMessageStatus> flushOKFuture = new CompletableFuture<>();
        private final long startTimestamp = System.currentTimeMillis();
        private long timeoutMillis = Long.MAX_VALUE;

        public GroupCommitRequest(long nextOffset, long timeoutMillis) {
            this.nextOffset = nextOffset;
            this.timeoutMillis = timeoutMillis;
        }

        public GroupCommitRequest(long nextOffset) {
            this.nextOffset = nextOffset;
        }


        public long getNextOffset() {
            return nextOffset;
        }

        public void wakeupCustomer(final PutMessageStatus putMessageStatus) {
            this.flushOKFuture.complete(putMessageStatus);
        }

        public CompletableFuture<PutMessageStatus> future() {
            return flushOKFuture;
        }

    }

    /**
     * GroupCommit Service
     * 同步刷盘服务
     */
    class GroupCommitService extends FlushCommitLogService {
        /**
         * 要进行刷盘的请求，在CommitLog写入之后会将刷盘请求放到此列表中
         */
        private volatile LinkedList<GroupCommitRequest> requestsWrite = new LinkedList<GroupCommitRequest>();
        // 读请求队列
        private volatile LinkedList<GroupCommitRequest> requestsRead = new LinkedList<GroupCommitRequest>();
        private final PutMessageSpinLock lock = new PutMessageSpinLock();

        /**
         * 将刷盘请求提交给同步刷盘服务，并唤醒刷盘服务线程进行刷盘
         * @param request
         */
        public synchronized void putRequest(final GroupCommitRequest request) {
            lock.lock();
            try {
                // 将刷盘请求添加到requestsWrite列表中
                this.requestsWrite.add(request);
            } finally {
                lock.unlock();
            }
            // 唤醒阻塞的刷盘服务，阻塞的刷盘服务线程唤醒后，会执行onWaitEnd方法，会交换requestsRead和requestsWrite的数据
            this.wakeup();
        }

        /**
         * 会交换requestsRead和requestsWrite的数据
         */
        private void swapRequests() {
            lock.lock();
            try {
                LinkedList<GroupCommitRequest> tmp = this.requestsWrite;
                this.requestsWrite = this.requestsRead;
                this.requestsRead = tmp;
            } finally {
                lock.unlock();
            }
        }

        /**
         * 同步刷盘操作
         */
        private void doCommit() {
            // 此时requestRead中存放的是刷盘请求GroupCommitRequest
            if (!this.requestsRead.isEmpty()) {
                for (GroupCommitRequest req : this.requestsRead) {
                    // There may be a message in the next file, so a maximum of
                    // two times the flush
                    // 上一次刷盘的位置大于等于nextOffset，说明nextOffset开始的数据已经被刷过了，不需要再刷新
                    boolean flushOK = CommitLog.this.mappedFileQueue.getFlushedWhere() >= req.getNextOffset();
                    for (int i = 0; i < 2 && !flushOK; i++) {
                        // 刷盘，参数为0说明是同步刷盘
                        CommitLog.this.mappedFileQueue.flush(0);
                        flushOK = CommitLog.this.mappedFileQueue.getFlushedWhere() >= req.getNextOffset();
                    }

                    // 通知刷盘完成
                    req.wakeupCustomer(flushOK ? PutMessageStatus.PUT_OK : PutMessageStatus.FLUSH_DISK_TIMEOUT);
                }

                long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
                if (storeTimestamp > 0) {
                    CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                }

                this.requestsRead = new LinkedList<>();
            } else {
                // Because of individual messages is set to not sync flush, it
                // will come to this process
                CommitLog.this.mappedFileQueue.flush(0);
            }
        }

        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    /*
                        waitForRunning的意思是让线程阻塞一段时间，到了时间后继续运行，或者调用了wakeup进行唤醒。
                        也就是说一次异步刷盘操作后会暂停500ms后在进行刷盘，或者是在暂停的时候被wakeup唤醒了，也可
                        以继续进行刷盘。

                        在有新消息写到commitlog中的时候，会调用wakeup方法唤醒当前刷盘线程。
                     */
                    this.waitForRunning(10);
                    // 刷盘操作
                    this.doCommit();
                } catch (Exception e) {
                    CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            // Under normal circumstances shutdown, wait for the arrival of the
            // request, and then flush
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                CommitLog.log.warn("GroupCommitService Exception, ", e);
            }

            synchronized (this) {
                this.swapRequests();
            }

            this.doCommit();

            CommitLog.log.info(this.getServiceName() + " service end");
        }

        @Override
        protected void onWaitEnd() {
            // 交换requestsRead和requestsWrite的数据
            this.swapRequests();
        }

        @Override
        public String getServiceName() {
            return GroupCommitService.class.getSimpleName();
        }

        @Override
        public long getJointime() {
            return 1000 * 60 * 5;
        }
    }

    class DefaultAppendMessageCallback implements AppendMessageCallback {
        // File at the end of the minimum fixed length empty
        private static final int END_FILE_MIN_BLANK_LENGTH = 4 + 4;
        private final ByteBuffer msgIdMemory;
        private final ByteBuffer msgIdV6Memory;
        // Store the message content
        private final ByteBuffer msgStoreItemMemory;
        // The maximum length of the message
        private final int maxMessageSize;

        DefaultAppendMessageCallback(final int size) {
            this.msgIdMemory = ByteBuffer.allocate(4 + 4 + 8);
            this.msgIdV6Memory = ByteBuffer.allocate(16 + 4 + 8);
            this.msgStoreItemMemory = ByteBuffer.allocate(END_FILE_MIN_BLANK_LENGTH);
            this.maxMessageSize = size;
        }

        /**
         * 追加消息
         * @param fileFromOffset
         * @param byteBuffer
         * @param maxBlank
         * @param msgInner
         * @param putMessageContext
         * @return
         */
        public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank,
            final MessageExtBrokerInner msgInner, PutMessageContext putMessageContext) {
            // STORETIMESTAMP + STOREHOSTADDRESS + OFFSET <br>

            // PHY OFFSET
            long wroteOffset = fileFromOffset + byteBuffer.position();

            Supplier<String> msgIdSupplier = () -> {
                int sysflag = msgInner.getSysFlag();
                int msgIdLen = (sysflag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 + 8 : 16 + 4 + 8;
                ByteBuffer msgIdBuffer = ByteBuffer.allocate(msgIdLen);
                MessageExt.socketAddress2ByteBuffer(msgInner.getStoreHost(), msgIdBuffer);
                msgIdBuffer.clear();//because socketAddress2ByteBuffer flip the buffer
                msgIdBuffer.putLong(msgIdLen - 8, wroteOffset);
                return UtilAll.bytes2string(msgIdBuffer.array());
            };

            // Record ConsumeQueue information
            String key = putMessageContext.getTopicQueueTableKey();
            Long queueOffset = CommitLog.this.topicQueueTable.get(key);
            if (null == queueOffset) {
                queueOffset = 0L;
                CommitLog.this.topicQueueTable.put(key, queueOffset);
            }

            // Transaction messages that require special handling
            // 事务消息需要特殊处理
            final int tranType = MessageSysFlag.getTransactionValue(msgInner.getSysFlag());
            switch (tranType) {
                // Prepared and Rollback message is not consumed, will not enter the
                // consumer queuec
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    queueOffset = 0L;
                    break;
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                default:
                    break;
            }

            ByteBuffer preEncodeBuffer = msgInner.getEncodedBuff();
            final int msgLen = preEncodeBuffer.getInt(0);

            // Determines whether there is sufficient free space
            // 消息长度加上END_FILE_MIN_BLANK_LENGTH大于CommitLog文件的空闲空间，
            // 返回END_OF_FILE，Broker会重新创建一个新的CommitLog文件来存储该消息
            // 每个CommitLog文件至少空闲8字节，高4字节存储当前文件剩余空间，低4字节存储魔数：BLANK_MAGIC_CODE
            if ((msgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
                this.msgStoreItemMemory.clear();
                // 1 TOTALSIZE
                this.msgStoreItemMemory.putInt(maxBlank);
                // 2 MAGICCODE
                this.msgStoreItemMemory.putInt(CommitLog.BLANK_MAGIC_CODE);
                // 3 The remaining space may be any value
                // Here the length of the specially set maxBlank
                final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
                byteBuffer.put(this.msgStoreItemMemory.array(), 0, 8);
                return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset,
                        maxBlank, /* only wrote 8 bytes, but declare wrote maxBlank for compute write position */
                        msgIdSupplier, msgInner.getStoreTimestamp(),
                        queueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
            }

            int pos = 4 + 4 + 4 + 4 + 4;
            // 6 QUEUEOFFSET
            preEncodeBuffer.putLong(pos, queueOffset);
            pos += 8;
            // 7 PHYSICALOFFSET
            preEncodeBuffer.putLong(pos, fileFromOffset + byteBuffer.position());
            int ipLen = (msgInner.getSysFlag() & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            // 8 SYSFLAG, 9 BORNTIMESTAMP, 10 BORNHOST, 11 STORETIMESTAMP
            pos += 8 + 4 + 8 + ipLen;
            // refresh store time stamp in lock
            preEncodeBuffer.putLong(pos, msgInner.getStoreTimestamp());


            final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
            // Write messages to the queue buffer
            // 将消息内容存储到ByteBuffer中，此时没有刷盘
            byteBuffer.put(preEncodeBuffer);
            msgInner.setEncodedBuff(null);
            // 创建结果
            AppendMessageResult result = new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, msgLen, msgIdSupplier,
                msgInner.getStoreTimestamp(), queueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);

            switch (tranType) {
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    break;
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                    // The next update ConsumeQueue information
                    CommitLog.this.topicQueueTable.put(key, ++queueOffset);
                    break;
                default:
                    break;
            }
            return result;
        }

        public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank,
            final MessageExtBatch messageExtBatch, PutMessageContext putMessageContext) {
            byteBuffer.mark();
            //physical offset
            long wroteOffset = fileFromOffset + byteBuffer.position();
            // Record ConsumeQueue information
            String key = putMessageContext.getTopicQueueTableKey();
            Long queueOffset = CommitLog.this.topicQueueTable.get(key);
            if (null == queueOffset) {
                queueOffset = 0L;
                CommitLog.this.topicQueueTable.put(key, queueOffset);
            }
            long beginQueueOffset = queueOffset;
            int totalMsgLen = 0;
            int msgNum = 0;

            final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
            ByteBuffer messagesByteBuff = messageExtBatch.getEncodedBuff();

            int sysFlag = messageExtBatch.getSysFlag();
            int bornHostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            int storeHostLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            Supplier<String> msgIdSupplier = () -> {
                int msgIdLen = storeHostLength + 8;
                int batchCount = putMessageContext.getBatchSize();
                long[] phyPosArray = putMessageContext.getPhyPos();
                ByteBuffer msgIdBuffer = ByteBuffer.allocate(msgIdLen);
                MessageExt.socketAddress2ByteBuffer(messageExtBatch.getStoreHost(), msgIdBuffer);
                msgIdBuffer.clear();//because socketAddress2ByteBuffer flip the buffer

                StringBuilder buffer = new StringBuilder(batchCount * msgIdLen * 2 + batchCount - 1);
                for (int i = 0; i < phyPosArray.length; i++) {
                    msgIdBuffer.putLong(msgIdLen - 8, phyPosArray[i]);
                    String msgId = UtilAll.bytes2string(msgIdBuffer.array());
                    if (i != 0) {
                        buffer.append(',');
                    }
                    buffer.append(msgId);
                }
                return buffer.toString();
            };

            messagesByteBuff.mark();
            int index = 0;
            while (messagesByteBuff.hasRemaining()) {
                // 1 TOTALSIZE
                final int msgPos = messagesByteBuff.position();
                final int msgLen = messagesByteBuff.getInt();
                final int bodyLen = msgLen - 40; //only for log, just estimate it
                // Exceeds the maximum message
                if (msgLen > this.maxMessageSize) {
                    CommitLog.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLen
                        + ", maxMessageSize: " + this.maxMessageSize);
                    return new AppendMessageResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED);
                }
                totalMsgLen += msgLen;
                // Determines whether there is sufficient free space
                if ((totalMsgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
                    this.msgStoreItemMemory.clear();
                    // 1 TOTALSIZE
                    this.msgStoreItemMemory.putInt(maxBlank);
                    // 2 MAGICCODE
                    this.msgStoreItemMemory.putInt(CommitLog.BLANK_MAGIC_CODE);
                    // 3 The remaining space may be any value
                    //ignore previous read
                    messagesByteBuff.reset();
                    // Here the length of the specially set maxBlank
                    byteBuffer.reset(); //ignore the previous appended messages
                    byteBuffer.put(this.msgStoreItemMemory.array(), 0, 8);
                    return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset, maxBlank, msgIdSupplier, messageExtBatch.getStoreTimestamp(),
                        beginQueueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
                }
                //move to add queue offset and commitlog offset
                int pos = msgPos + 20;
                messagesByteBuff.putLong(pos, queueOffset);
                pos += 8;
                messagesByteBuff.putLong(pos, wroteOffset + totalMsgLen - msgLen);
                // 8 SYSFLAG, 9 BORNTIMESTAMP, 10 BORNHOST, 11 STORETIMESTAMP
                pos += 8 + 4 + 8 + bornHostLength;
                // refresh store time stamp in lock
                messagesByteBuff.putLong(pos, messageExtBatch.getStoreTimestamp());

                putMessageContext.getPhyPos()[index++] = wroteOffset + totalMsgLen - msgLen;
                queueOffset++;
                msgNum++;
                messagesByteBuff.position(msgPos + msgLen);
            }

            messagesByteBuff.position(0);
            messagesByteBuff.limit(totalMsgLen);
            byteBuffer.put(messagesByteBuff);
            messageExtBatch.setEncodedBuff(null);
            AppendMessageResult result = new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, totalMsgLen, msgIdSupplier,
                messageExtBatch.getStoreTimestamp(), beginQueueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
            result.setMsgNum(msgNum);
            CommitLog.this.topicQueueTable.put(key, queueOffset);

            return result;
        }

        private void resetByteBuffer(final ByteBuffer byteBuffer, final int limit) {
            byteBuffer.flip();
            byteBuffer.limit(limit);
        }

    }

    public static class MessageExtEncoder {
        // Store the message content
        private final ByteBuffer encoderBuffer;
        // The maximum length of the message
        private final int maxMessageSize;

        MessageExtEncoder(final int size) {
            this.encoderBuffer = ByteBuffer.allocateDirect(size);
            this.maxMessageSize = size;
        }

        private void socketAddress2ByteBuffer(final SocketAddress socketAddress, final ByteBuffer byteBuffer) {
            InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
            InetAddress address = inetSocketAddress.getAddress();
            if (address instanceof Inet4Address) {
                byteBuffer.put(inetSocketAddress.getAddress().getAddress(), 0, 4);
            } else {
                byteBuffer.put(inetSocketAddress.getAddress().getAddress(), 0, 16);
            }
            byteBuffer.putInt(inetSocketAddress.getPort());
        }

        protected PutMessageResult encode(MessageExtBrokerInner msgInner) {
            /**
             * Serialize message
             */
            final byte[] propertiesData =
                    msgInner.getPropertiesString() == null ? null : msgInner.getPropertiesString().getBytes(MessageDecoder.CHARSET_UTF8);

            final int propertiesLength = propertiesData == null ? 0 : propertiesData.length;

            if (propertiesLength > Short.MAX_VALUE) {
                log.warn("putMessage message properties length too long. length={}", propertiesData.length);
                return new PutMessageResult(PutMessageStatus.PROPERTIES_SIZE_EXCEEDED, null);
            }

            final byte[] topicData = msgInner.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);
            final int topicLength = topicData.length;

            final int bodyLength = msgInner.getBody() == null ? 0 : msgInner.getBody().length;

            final int msgLen = calMsgLength(msgInner.getSysFlag(), bodyLength, topicLength, propertiesLength);

            // Exceeds the maximum message
            if (msgLen > this.maxMessageSize) {
                CommitLog.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLength
                        + ", maxMessageSize: " + this.maxMessageSize);
                return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
            }

            // Initialization of storage space
            this.resetByteBuffer(encoderBuffer, msgLen);
            // 1 TOTALSIZE
            this.encoderBuffer.putInt(msgLen);
            // 2 MAGICCODE
            this.encoderBuffer.putInt(CommitLog.MESSAGE_MAGIC_CODE);
            // 3 BODYCRC
            this.encoderBuffer.putInt(msgInner.getBodyCRC());
            // 4 QUEUEID
            this.encoderBuffer.putInt(msgInner.getQueueId());
            // 5 FLAG
            this.encoderBuffer.putInt(msgInner.getFlag());
            // 6 QUEUEOFFSET, need update later
            this.encoderBuffer.putLong(0);
            // 7 PHYSICALOFFSET, need update later
            this.encoderBuffer.putLong(0);
            // 8 SYSFLAG
            this.encoderBuffer.putInt(msgInner.getSysFlag());
            // 9 BORNTIMESTAMP
            this.encoderBuffer.putLong(msgInner.getBornTimestamp());
            // 10 BORNHOST
            socketAddress2ByteBuffer(msgInner.getBornHost() ,this.encoderBuffer);
            // 11 STORETIMESTAMP
            this.encoderBuffer.putLong(msgInner.getStoreTimestamp());
            // 12 STOREHOSTADDRESS
            socketAddress2ByteBuffer(msgInner.getStoreHost() ,this.encoderBuffer);
            // 13 RECONSUMETIMES
            this.encoderBuffer.putInt(msgInner.getReconsumeTimes());
            // 14 Prepared Transaction Offset
            this.encoderBuffer.putLong(msgInner.getPreparedTransactionOffset());
            // 15 BODY
            this.encoderBuffer.putInt(bodyLength);
            if (bodyLength > 0)
                this.encoderBuffer.put(msgInner.getBody());
            // 16 TOPIC
            this.encoderBuffer.put((byte) topicLength);
            this.encoderBuffer.put(topicData);
            // 17 PROPERTIES
            this.encoderBuffer.putShort((short) propertiesLength);
            if (propertiesLength > 0)
                this.encoderBuffer.put(propertiesData);

            encoderBuffer.flip();
            return null;
        }

        protected ByteBuffer encode(final MessageExtBatch messageExtBatch, PutMessageContext putMessageContext) {
            encoderBuffer.clear(); //not thread-safe
            int totalMsgLen = 0;
            ByteBuffer messagesByteBuff = messageExtBatch.wrap();

            int sysFlag = messageExtBatch.getSysFlag();
            int bornHostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            int storeHostLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            ByteBuffer bornHostHolder = ByteBuffer.allocate(bornHostLength);
            ByteBuffer storeHostHolder = ByteBuffer.allocate(storeHostLength);

            // properties from MessageExtBatch
            String batchPropStr = MessageDecoder.messageProperties2String(messageExtBatch.getProperties());
            final byte[] batchPropData = batchPropStr.getBytes(MessageDecoder.CHARSET_UTF8);
            final short batchPropLen = (short) batchPropData.length;

            int batchSize = 0;
            while (messagesByteBuff.hasRemaining()) {
                batchSize++;
                // 1 TOTALSIZE
                messagesByteBuff.getInt();
                // 2 MAGICCODE
                messagesByteBuff.getInt();
                // 3 BODYCRC
                messagesByteBuff.getInt();
                // 4 FLAG
                int flag = messagesByteBuff.getInt();
                // 5 BODY
                int bodyLen = messagesByteBuff.getInt();
                int bodyPos = messagesByteBuff.position();
                int bodyCrc = UtilAll.crc32(messagesByteBuff.array(), bodyPos, bodyLen);
                messagesByteBuff.position(bodyPos + bodyLen);
                // 6 properties
                short propertiesLen = messagesByteBuff.getShort();
                int propertiesPos = messagesByteBuff.position();
                messagesByteBuff.position(propertiesPos + propertiesLen);

                final byte[] topicData = messageExtBatch.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);

                final int topicLength = topicData.length;

                final int msgLen = calMsgLength(messageExtBatch.getSysFlag(), bodyLen, topicLength,
                        propertiesLen + batchPropLen);

                // Exceeds the maximum message
                if (msgLen > this.maxMessageSize) {
                    CommitLog.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLen
                        + ", maxMessageSize: " + this.maxMessageSize);
                    throw new RuntimeException("message size exceeded");
                }

                totalMsgLen += msgLen;
                // Determines whether there is sufficient free space
                if (totalMsgLen > maxMessageSize) {
                    throw new RuntimeException("message size exceeded");
                }

                // 1 TOTALSIZE
                this.encoderBuffer.putInt(msgLen);
                // 2 MAGICCODE
                this.encoderBuffer.putInt(CommitLog.MESSAGE_MAGIC_CODE);
                // 3 BODYCRC
                this.encoderBuffer.putInt(bodyCrc);
                // 4 QUEUEID
                this.encoderBuffer.putInt(messageExtBatch.getQueueId());
                // 5 FLAG
                this.encoderBuffer.putInt(flag);
                // 6 QUEUEOFFSET
                this.encoderBuffer.putLong(0);
                // 7 PHYSICALOFFSET
                this.encoderBuffer.putLong(0);
                // 8 SYSFLAG
                this.encoderBuffer.putInt(messageExtBatch.getSysFlag());
                // 9 BORNTIMESTAMP
                this.encoderBuffer.putLong(messageExtBatch.getBornTimestamp());
                // 10 BORNHOST
                this.resetByteBuffer(bornHostHolder, bornHostLength);
                this.encoderBuffer.put(messageExtBatch.getBornHostBytes(bornHostHolder));
                // 11 STORETIMESTAMP
                this.encoderBuffer.putLong(messageExtBatch.getStoreTimestamp());
                // 12 STOREHOSTADDRESS
                this.resetByteBuffer(storeHostHolder, storeHostLength);
                this.encoderBuffer.put(messageExtBatch.getStoreHostBytes(storeHostHolder));
                // 13 RECONSUMETIMES
                this.encoderBuffer.putInt(messageExtBatch.getReconsumeTimes());
                // 14 Prepared Transaction Offset, batch does not support transaction
                this.encoderBuffer.putLong(0);
                // 15 BODY
                this.encoderBuffer.putInt(bodyLen);
                if (bodyLen > 0)
                    this.encoderBuffer.put(messagesByteBuff.array(), bodyPos, bodyLen);
                // 16 TOPIC
                this.encoderBuffer.put((byte) topicLength);
                this.encoderBuffer.put(topicData);
                // 17 PROPERTIES
                this.encoderBuffer.putShort((short) (propertiesLen + batchPropLen));
                if (propertiesLen > 0) {
                    this.encoderBuffer.put(messagesByteBuff.array(), propertiesPos, propertiesLen);
                }
                if (batchPropLen > 0) {
                    this.encoderBuffer.put(batchPropData, 0, batchPropLen);
                }
            }
            putMessageContext.setBatchSize(batchSize);
            putMessageContext.setPhyPos(new long[batchSize]);
            encoderBuffer.flip();
            return encoderBuffer;
        }

        private void resetByteBuffer(final ByteBuffer byteBuffer, final int limit) {
            byteBuffer.flip();
            byteBuffer.limit(limit);
        }

    }

    static class PutMessageThreadLocal {
        private MessageExtEncoder encoder;
        private StringBuilder keyBuilder;
        PutMessageThreadLocal(int size) {
            encoder = new MessageExtEncoder(size);
            keyBuilder = new StringBuilder();
        }

        public MessageExtEncoder getEncoder() {
            return encoder;
        }

        public StringBuilder getKeyBuilder() {
            return keyBuilder;
        }
    }

    static class PutMessageContext {
        private String topicQueueTableKey;
        private long[] phyPos;
        private int batchSize;

        public PutMessageContext(String topicQueueTableKey) {
            this.topicQueueTableKey = topicQueueTableKey;
        }

        public String getTopicQueueTableKey() {
            return topicQueueTableKey;
        }

        public long[] getPhyPos() {
            return phyPos;
        }

        public void setPhyPos(long[] phyPos) {
            this.phyPos = phyPos;
        }

        public int getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }
    }
}
