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
package org.apache.rocketmq.client.impl.producer;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.client.common.ThreadLocalIndex;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;

public class TopicPublishInfo {
    /**
     * 是否顺序消息
     */
    private boolean orderTopic = false;
    private boolean haveTopicRouterInfo = false;

    /**
     * 该主题对应的消息队列
     */
    private List<MessageQueue> messageQueueList = new ArrayList<MessageQueue>();

    /**
     * 用于选择消息队列
     * 每选择一次消息队列，该值会自增1
     */
    private volatile ThreadLocalIndex sendWhichQueue = new ThreadLocalIndex();

    /**
     * 路由结果信息
     */
    private TopicRouteData topicRouteData;

    public boolean isOrderTopic() {
        return orderTopic;
    }

    public void setOrderTopic(boolean orderTopic) {
        this.orderTopic = orderTopic;
    }

    public boolean ok() {
        return null != this.messageQueueList && !this.messageQueueList.isEmpty();
    }

    public List<MessageQueue> getMessageQueueList() {
        return messageQueueList;
    }

    public void setMessageQueueList(List<MessageQueue> messageQueueList) {
        this.messageQueueList = messageQueueList;
    }

    public ThreadLocalIndex getSendWhichQueue() {
        return sendWhichQueue;
    }

    public void setSendWhichQueue(ThreadLocalIndex sendWhichQueue) {
        this.sendWhichQueue = sendWhichQueue;
    }

    public boolean isHaveTopicRouterInfo() {
        return haveTopicRouterInfo;
    }

    public void setHaveTopicRouterInfo(boolean haveTopicRouterInfo) {
        this.haveTopicRouterInfo = haveTopicRouterInfo;
    }

    /**
     * 如果没有启用broker故障延迟机制，选择调用的队列
     * 在一次消息发送过程中，可能会多次执行选择消息队列的方法，
     * lastBrokerName就是上次选择的执行发送消息失败的Broker。
     * 第一次执行消息队列选择时，lastBrokerName为null，此时直接使用sendWhichQueue自增再获取，
     * 与当前路由表中消息队列个数取模，返回该位置的MessageQueue，如果消息发送失败的话，下次进行消息队列选择时
     * 规避上次MessageQueue所在的Broker，否则还是有可能再次失败。
     *
     * 该算法在一次消息发送过程中能成功规避故障的Broker，但是如果Broker宕机，由于路由算法中的消息队列是按Broker排序的，
     * 如果上一次根据路由算法选择的是宕机的Broker的第一个队列，那么随后的下次选择的是宕机的Broker的第二个队列，
     * 消息发送很有可能会失败，再次引发重试。
     *
     * 可以在一次消息发送失败后，将不可用的Broker排除在消息队列选择范围外。Broker故障延迟机制可以做到。
     * @param lastBrokerName
     * @return
     */
    public MessageQueue selectOneMessageQueue(final String lastBrokerName) {
        // lastBrokerName是上一次执行消息发送时选择失败的broker
        // 在重试机制下，第一次执行消息发送时，lastBrokerName = null
        if (lastBrokerName == null) {
            return selectOneMessageQueue();
        } else {
            for (int i = 0; i < this.messageQueueList.size(); i++) {
                // 如果选择的队列发送消息发送失败了，此时重试机制再次选择队列时lastBrokerName不为空
                int index = this.sendWhichQueue.incrementAndGet();
                int pos = Math.abs(index) % this.messageQueueList.size();
                if (pos < 0)
                    pos = 0;
                MessageQueue mq = this.messageQueueList.get(pos);
                // 上面还是先循环选择一个队列，但是如果是上一次选择的失败的broker，则继续选择下一个
                if (!mq.getBrokerName().equals(lastBrokerName)) {
                    return mq;
                }
            }
            return selectOneMessageQueue();
        }
    }

    public MessageQueue selectOneMessageQueue() {
        // sendWhichQueue是一个ThreadLocal，存储自增值，自增值第一次使用Random类随机取值
        // 如果消息发送触发了重试机制，每次就自增取值
        int index = this.sendWhichQueue.incrementAndGet();
        // 自增值和消息队列长度取模，目的是为了循环选择消息队列。
        int pos = Math.abs(index) % this.messageQueueList.size();
        if (pos < 0)
            pos = 0;
        return this.messageQueueList.get(pos);
    }

    public int getQueueIdByBroker(final String brokerName) {
        for (int i = 0; i < topicRouteData.getQueueDatas().size(); i++) {
            final QueueData queueData = this.topicRouteData.getQueueDatas().get(i);
            if (queueData.getBrokerName().equals(brokerName)) {
                return queueData.getWriteQueueNums();
            }
        }

        return -1;
    }

    @Override
    public String toString() {
        return "TopicPublishInfo [orderTopic=" + orderTopic + ", messageQueueList=" + messageQueueList
            + ", sendWhichQueue=" + sendWhichQueue + ", haveTopicRouterInfo=" + haveTopicRouterInfo + "]";
    }

    public TopicRouteData getTopicRouteData() {
        return topicRouteData;
    }

    public void setTopicRouteData(final TopicRouteData topicRouteData) {
        this.topicRouteData = topicRouteData;
    }
}
