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
package org.apache.rocketmq.client.consumer.rebalance;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * Average Hashing queue algorithm
 */
public class AllocateMessageQueueAveragely implements AllocateMessageQueueStrategy {
    private final InternalLogger log = ClientLogger.getLog();

    /**
     * 平均分配
     * 比如有消费者 1,2,3,4,5
     * 有队列mq_1,mq_2,mq_3,mq_4,mq_5,mq_6,mq_7,mq_8
     * 分配结果是：1消费mq_1,mq_2; 2消费mq_3,mq_4; 3消费mq_5,mq_6; 4消费mq_7; 5消费mq_8
     *
     * @param consumerGroup current consumer group 当前消费组
     * @param currentCID current consumer id 当前消费者id
     * @param mqAll message queue set in current topic topic对应的所有消息队列
     * @param cidAll consumer set in current consumer group 当前消费组下所有的消费者
     * @return
     */
    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll,
        List<String> cidAll) {
        if (currentCID == null || currentCID.length() < 1) {
            throw new IllegalArgumentException("currentCID is empty");
        }
        if (mqAll == null || mqAll.isEmpty()) {
            throw new IllegalArgumentException("mqAll is null or mqAll empty");
        }
        if (cidAll == null || cidAll.isEmpty()) {
            throw new IllegalArgumentException("cidAll is null or cidAll empty");
        }

        List<MessageQueue> result = new ArrayList<MessageQueue>();
        if (!cidAll.contains(currentCID)) {
            log.info("[BUG] ConsumerGroup: {} The consumerId: {} not in cidAll: {}",
                consumerGroup,
                currentCID,
                cidAll);
            return result;
        }

        int index = cidAll.indexOf(currentCID);
        int mod = mqAll.size() % cidAll.size();
        int averageSize =
            mqAll.size() <= cidAll.size() ? 1 : (mod > 0 && index < mod ? mqAll.size() / cidAll.size()
                + 1 : mqAll.size() / cidAll.size());
        int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
        int range = Math.min(averageSize, mqAll.size() - startIndex);
        for (int i = 0; i < range; i++) {
            result.add(mqAll.get((startIndex + i) % mqAll.size()));
        }
        return result;
    }

    public static void main(String[] args) {
        // int currentCID = 104;
        List<Integer> cidAll = new ArrayList<Integer>();
        cidAll.add(1);
        cidAll.add(2);
        cidAll.add(3);
        cidAll.add(4);

        List<String> mqAll = new ArrayList<String>();
        mqAll.add("mq_0");
        mqAll.add("mq_1");
        mqAll.add("mq_2");
        mqAll.add("mq_3");
        mqAll.add("mq_4");
        mqAll.add("mq_5");
        mqAll.add("mq_6");
        mqAll.add("mq_7");
        mqAll.add("mq_8");
        mqAll.add("mq_9");


        for (Integer currentCID : cidAll) {
            System.out.println("currentCID: " + currentCID);
            List<String> result = new ArrayList<String>();

            int index = cidAll.indexOf(currentCID);
            System.out.println("index: " + index);
            int mod = mqAll.size() % cidAll.size();
            System.out.println("mod:" + mod);
            int averageSize =
                mqAll.size() <= cidAll.size() ? 1 : (mod > 0 && index < mod ? mqAll.size() / cidAll.size()
                    + 1 : mqAll.size() / cidAll.size());
            System.out.println("averageSize: " + averageSize);
            int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
            System.out.println("startIndex: " + startIndex);
            int range = Math.min(averageSize, mqAll.size() - startIndex);
            System.out.println("range: " + range);
            for (int i = 0; i < range; i++) {
                result.add(mqAll.get((startIndex + i) % mqAll.size()));
            }

            System.out.println(result);
            System.out.println("========");
        }

    }

    @Override
    public String getName() {
        return "AVG";
    }
}
