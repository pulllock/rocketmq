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
package org.apache.rocketmq.broker.client;

import io.netty.channel.Channel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

/**
 * 消费组信息
 */
public class ConsumerGroupInfo {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    /**
     * 消费组名
     */
    private final String groupName;

    /**
     * 订阅信息
     * key topic
     * value 订阅数据
     */
    private final ConcurrentMap<String/* Topic */, SubscriptionData> subscriptionTable =
        new ConcurrentHashMap<String, SubscriptionData>();
    private final ConcurrentMap<Channel, ClientChannelInfo> channelInfoTable =
        new ConcurrentHashMap<Channel, ClientChannelInfo>(16);
    private volatile ConsumeType consumeType;
    private volatile MessageModel messageModel;
    private volatile ConsumeFromWhere consumeFromWhere;
    private volatile long lastUpdateTimestamp = System.currentTimeMillis();

    public ConsumerGroupInfo(String groupName, ConsumeType consumeType, MessageModel messageModel,
        ConsumeFromWhere consumeFromWhere) {
        this.groupName = groupName;
        this.consumeType = consumeType;
        this.messageModel = messageModel;
        this.consumeFromWhere = consumeFromWhere;
    }

    public ClientChannelInfo findChannel(final String clientId) {
        Iterator<Entry<Channel, ClientChannelInfo>> it = this.channelInfoTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Channel, ClientChannelInfo> next = it.next();
            if (next.getValue().getClientId().equals(clientId)) {
                return next.getValue();
            }
        }

        return null;
    }

    public ConcurrentMap<String, SubscriptionData> getSubscriptionTable() {
        return subscriptionTable;
    }

    public ConcurrentMap<Channel, ClientChannelInfo> getChannelInfoTable() {
        return channelInfoTable;
    }

    public List<Channel> getAllChannel() {
        List<Channel> result = new ArrayList<>();

        result.addAll(this.channelInfoTable.keySet());

        return result;
    }

    public List<String> getAllClientId() {
        List<String> result = new ArrayList<>();

        Iterator<Entry<Channel, ClientChannelInfo>> it = this.channelInfoTable.entrySet().iterator();

        while (it.hasNext()) {
            Entry<Channel, ClientChannelInfo> entry = it.next();
            ClientChannelInfo clientChannelInfo = entry.getValue();
            result.add(clientChannelInfo.getClientId());
        }

        return result;
    }

    public void unregisterChannel(final ClientChannelInfo clientChannelInfo) {
        ClientChannelInfo old = this.channelInfoTable.remove(clientChannelInfo.getChannel());
        if (old != null) {
            log.info("unregister a consumer[{}] from consumerGroupInfo {}", this.groupName, old.toString());
        }
    }

    public boolean doChannelCloseEvent(final String remoteAddr, final Channel channel) {
        final ClientChannelInfo info = this.channelInfoTable.remove(channel);
        if (info != null) {
            log.warn(
                "NETTY EVENT: remove not active channel[{}] from ConsumerGroupInfo groupChannelTable, consumer group: {}",
                info.toString(), groupName);
            return true;
        }

        return false;
    }

    /**
     * 更新channel
     * @param infoNew 新的channel
     * @param consumeType 消费类型：pull或push
     * @param messageModel 消费模式：集群或广播
     * @param consumeFromWhere 从哪里开始消费
     * @return 新来的消费者返回true；其他都返回false
     */
    public boolean updateChannel(final ClientChannelInfo infoNew, ConsumeType consumeType,
        MessageModel messageModel, ConsumeFromWhere consumeFromWhere) {
        boolean updated = false;
        this.consumeType = consumeType;
        this.messageModel = messageModel;
        this.consumeFromWhere = consumeFromWhere;

        // 从channelInfo表获取
        ClientChannelInfo infoOld = this.channelInfoTable.get(infoNew.getChannel());
        // 表中没有channelInfo
        if (null == infoOld) {
            // 说明是新的消费者发来的
            ClientChannelInfo prev = this.channelInfoTable.put(infoNew.getChannel(), infoNew);
            if (null == prev) {
                log.info("new consumer connected, group: {} {} {} channel: {}", this.groupName, consumeType,
                    messageModel, infoNew.toString());
                // 有更新
                updated = true;
            }

            infoOld = infoNew;
        } else {
            // channelInfo表中存在
            // 表中存在的和参数中传进来的不是同一个
            if (!infoOld.getClientId().equals(infoNew.getClientId())) {
                log.error("[BUG] consumer channel exist in broker, but clientId not equal. GROUP: {} OLD: {} NEW: {} ",
                    this.groupName,
                    infoOld.toString(),
                    infoNew.toString());
                this.channelInfoTable.put(infoNew.getChannel(), infoNew);
            }
        }

        this.lastUpdateTimestamp = System.currentTimeMillis();
        infoOld.setLastUpdateTimestamp(this.lastUpdateTimestamp);

        return updated;
    }

    /**
     * 更新消费组内的订阅信息
     * @param subList 新的订阅信息
     * @return 新加入topic订阅信息或删除了topic订阅信息返回true；更新了topic信息或者原封未动则返回false
     */
    public boolean updateSubscription(final Set<SubscriptionData> subList) {
        boolean updated = false;

        // 遍历订阅信息
        for (SubscriptionData sub : subList) {
            // 查询缓存的订阅信息表中是否存在该topic的订阅信息
            SubscriptionData old = this.subscriptionTable.get(sub.getTopic());
            // 原来缓存中不存在，说明这个topic是该消费组中消费者新订阅的topic
            if (old == null) {
                SubscriptionData prev = this.subscriptionTable.putIfAbsent(sub.getTopic(), sub);
                if (null == prev) {
                    // 新加入了topic订阅信息
                    updated = true;
                    log.info("subscription changed, add new topic, group: {} {}",
                        this.groupName,
                        sub.toString());
                }
            }
            // 原来缓存中已经存在topic订阅的信息，新的版本比旧的版本大，有更新
            else if (sub.getSubVersion() > old.getSubVersion()) {
                if (this.consumeType == ConsumeType.CONSUME_PASSIVELY) {
                    log.info("subscription changed, group: {} OLD: {} NEW: {}",
                        this.groupName,
                        old.toString(),
                        sub.toString()
                    );
                }
                // 更新缓存
                this.subscriptionTable.put(sub.getTopic(), sub);
            }
        }

        // 现在的subscriptionTable中包含老的topic订阅信息、更新后的topic订阅信息、新增的topic订阅信息
        Iterator<Entry<String, SubscriptionData>> it = this.subscriptionTable.entrySet().iterator();
        // 下面循环是要找出已经删除的topic订阅信息
        while (it.hasNext()) {
            Entry<String, SubscriptionData> next = it.next();
            // 缓存中的数据
            String oldTopic = next.getKey();

            boolean exist = false;
            for (SubscriptionData sub : subList) {
                if (sub.getTopic().equals(oldTopic)) {
                    exist = true;
                    break;
                }
            }

            // 缓存中数据在新的订阅数据中不存在，说明缓存中存在的是已经删除的
            if (!exist) {
                log.warn("subscription changed, group: {} remove topic {} {}",
                    this.groupName,
                    oldTopic,
                    next.getValue().toString()
                );

                it.remove();
                updated = true;
            }
        }

        this.lastUpdateTimestamp = System.currentTimeMillis();

        return updated;
    }

    public Set<String> getSubscribeTopics() {
        return subscriptionTable.keySet();
    }

    public SubscriptionData findSubscriptionData(final String topic) {
        return this.subscriptionTable.get(topic);
    }

    public ConsumeType getConsumeType() {
        return consumeType;
    }

    public void setConsumeType(ConsumeType consumeType) {
        this.consumeType = consumeType;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public String getGroupName() {
        return groupName;
    }

    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }

    public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
    }

    public ConsumeFromWhere getConsumeFromWhere() {
        return consumeFromWhere;
    }

    public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
        this.consumeFromWhere = consumeFromWhere;
    }
}
