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

package org.apache.rocketmq.client.latency;

import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 消息发送容错处策略，默认情况下关闭 sendLatencyFaultEnable=false，不启动broker故障延迟机制
 */
public class MQFaultStrategy {
    private final static InternalLogger log = ClientLogger.getLog();

    /**
     * 故障延迟容错实现类
     */
    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();

    /**
     * 是否启用故障延迟机制
     */
    private boolean sendLatencyFaultEnable = false;

    /**
     * 根据currentLatency本次消息发送延迟，从latencyMax尾部向前找到第一个比currentLatency小的索引index，
     * 如果没有找到，则返回0.然后根据这个索引从notAvailableDuration数组中取出对应时间，在这个时长内，
     * Broker将设置为不可用。
     */
    private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
    private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};

    public long[] getNotAvailableDuration() {
        return notAvailableDuration;
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.notAvailableDuration = notAvailableDuration;
    }

    public long[] getLatencyMax() {
        return latencyMax;
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.latencyMax = latencyMax;
    }

    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        // 启用了broker故障延迟机制
        if (this.sendLatencyFaultEnable) {
            try {
                // 获取自增值，然后跟队列长度取模，来获取一个队列
                int index = tpInfo.getSendWhichQueue().incrementAndGet();
                for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
                    int pos = Math.abs(index++) % tpInfo.getMessageQueueList().size();
                    if (pos < 0)
                        pos = 0;
                    MessageQueue mq = tpInfo.getMessageQueueList().get(pos);
                    // 校验下队列是否可用
                    if (latencyFaultTolerance.isAvailable(mq.getBrokerName()))
                        // 如果选择的队列可用，直接返回
                        return mq;
                }

                // 如果所有队列都不可用，尝试从失败的broker列表中选择一个broker，假装可用
                final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();
                int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);
                if (writeQueueNums > 0) {
                    // 轮询的方式选择一个队列
                    final MessageQueue mq = tpInfo.selectOneMessageQueue();
                    if (notBestBroker != null) {
                        mq.setBrokerName(notBestBroker);
                        mq.setQueueId(tpInfo.getSendWhichQueue().incrementAndGet() % writeQueueNums);
                    }
                    return mq;
                } else {
                    latencyFaultTolerance.remove(notBestBroker);
                }
            } catch (Exception e) {
                log.error("Error occurred when selecting message queue", e);
            }

            // 如果上面都没选出来或者发生了异常，就按照轮询方式直接选一个出来
            return tpInfo.selectOneMessageQueue();
        }

        // 没有启用broker故障延迟机制，默认的机制，直接轮询取一个
        return tpInfo.selectOneMessageQueue(lastBrokerName);
    }

    /**
     *
     * @param brokerName
     * @param currentLatency 本次消息发送的延迟时间
     * @param isolation 表示broker是否需要规避，如果消息发送成功，该参数为false，表示broker无需规避；
     *                  如果消息发送失败，该参数为true，表示broker发生故障，需要规避
     */
    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        // 只有启用了broker故障延迟，才需要更新
        if (this.sendLatencyFaultEnable) {
            // 如果broker需要规避，消息发送延迟时间默认是30秒
            long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
            // 得到规避时长duration后，继续
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
        }
    }

    private long computeNotAvailableDuration(final long currentLatency) {
        // 从latencyMax数组的尾部向前找
        // 如果isolation为true，则该broker会得到一个10分钟的规避时长。
        // 如果isolation为false，规避时长得看消息发送的延迟时间是多少。
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            if (currentLatency >= latencyMax[i])
                return this.notAvailableDuration[i];
        }

        return 0;
    }
}
