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

package org.apache.rocketmq.common.protocol;

/**
 * 请求码
 */
public class RequestCode {

    /**
     * 发送消息
     */
    public static final int SEND_MESSAGE = 10;

    /**
     * 拉取消息
     */
    public static final int PULL_MESSAGE = 11;

    /**
     * 查询消息
     */
    public static final int QUERY_MESSAGE = 12;
    public static final int QUERY_BROKER_OFFSET = 13;
    public static final int QUERY_CONSUMER_OFFSET = 14;
    public static final int UPDATE_CONSUMER_OFFSET = 15;

    /**
     * 更新或新增Topic
     */
    public static final int UPDATE_AND_CREATE_TOPIC = 17;

    /**
     * 获取所有Topic配置
     */
    public static final int GET_ALL_TOPIC_CONFIG = 21;
    public static final int GET_TOPIC_CONFIG_LIST = 22;

    public static final int GET_TOPIC_NAME_LIST = 23;

    /**
     * 更新Broker配置
     */
    public static final int UPDATE_BROKER_CONFIG = 25;

    /**
     * 获取Broker配置
     */
    public static final int GET_BROKER_CONFIG = 26;

    public static final int TRIGGER_DELETE_FILES = 27;

    public static final int GET_BROKER_RUNTIME_INFO = 28;
    public static final int SEARCH_OFFSET_BY_TIMESTAMP = 29;
    public static final int GET_MAX_OFFSET = 30;
    public static final int GET_MIN_OFFSET = 31;

    public static final int GET_EARLIEST_MSG_STORETIME = 32;

    public static final int VIEW_MESSAGE_BY_ID = 33;

    /**
     * 心跳请求
     */
    public static final int HEART_BEAT = 34;

    public static final int UNREGISTER_CLIENT = 35;

    public static final int CONSUMER_SEND_MSG_BACK = 36;

    public static final int END_TRANSACTION = 37;

    /**
     * 获取消费组下所有的消费者ID
     */
    public static final int GET_CONSUMER_LIST_BY_GROUP = 38;

    /**
     * 检查事务状态
     */
    public static final int CHECK_TRANSACTION_STATE = 39;

    /**
     * 通知消费者，组里有消费者发生了变化
     */
    public static final int NOTIFY_CONSUMER_IDS_CHANGED = 40;

    public static final int LOCK_BATCH_MQ = 41;

    public static final int UNLOCK_BATCH_MQ = 42;
    public static final int GET_ALL_CONSUMER_OFFSET = 43;

    public static final int GET_ALL_DELAY_OFFSET = 45;

    public static final int CHECK_CLIENT_CONFIG = 46;

    public static final int UPDATE_AND_CREATE_ACL_CONFIG = 50;

    public static final int DELETE_ACL_CONFIG = 51;

    public static final int GET_BROKER_CLUSTER_ACL_INFO = 52;

    public static final int UPDATE_GLOBAL_WHITE_ADDRS_CONFIG = 53;

    public static final int GET_BROKER_CLUSTER_ACL_CONFIG = 54;

    /**
     * 设置KV配置，NameServer处理该请求
     */
    public static final int PUT_KV_CONFIG = 100;

    /**
     * 获取KV配置，NameServer处理该请求
     */
    public static final int GET_KV_CONFIG = 101;

    /**
     * 删除KV配置，NameServer处理该请求
     */
    public static final int DELETE_KV_CONFIG = 102;

    /**
     * 注册Broker，NameServer处理该请求
     */
    public static final int REGISTER_BROKER = 103;

    /**
     * 注销Broker，NameServer处理该请求
     */
    public static final int UNREGISTER_BROKER = 104;

    /**
     * 根据Topic获取路由信息，NameServer处理该请求
     */
    public static final int GET_ROUTEINFO_BY_TOPIC = 105;

    /**
     * 获取Broker集群信息，NameServer处理该请求
     */
    public static final int GET_BROKER_CLUSTER_INFO = 106;
    public static final int UPDATE_AND_CREATE_SUBSCRIPTIONGROUP = 200;
    public static final int GET_ALL_SUBSCRIPTIONGROUP_CONFIG = 201;
    public static final int GET_TOPIC_STATS_INFO = 202;
    public static final int GET_CONSUMER_CONNECTION_LIST = 203;
    public static final int GET_PRODUCER_CONNECTION_LIST = 204;

    /**
     * 擦除Broker的持久化标记，NameServer处理该请求
     */
    public static final int WIPE_WRITE_PERM_OF_BROKER = 205;

    /**
     * 从NameServer获取所有的Topic列表，NameServer处理该请求
     */
    public static final int GET_ALL_TOPIC_LIST_FROM_NAMESERVER = 206;

    public static final int DELETE_SUBSCRIPTIONGROUP = 207;
    public static final int GET_CONSUME_STATS = 208;

    public static final int SUSPEND_CONSUMER = 209;

    public static final int RESUME_CONSUMER = 210;
    public static final int RESET_CONSUMER_OFFSET_IN_CONSUMER = 211;
    public static final int RESET_CONSUMER_OFFSET_IN_BROKER = 212;

    public static final int ADJUST_CONSUMER_THREAD_POOL = 213;

    public static final int WHO_CONSUME_THE_MESSAGE = 214;

    /**
     * 删除Broker中的Topic
     */
    public static final int DELETE_TOPIC_IN_BROKER = 215;

    /**
     * 删除NameServer中的Topic，NameServer处理该请求
     */
    public static final int DELETE_TOPIC_IN_NAMESRV = 216;

    /**
     * 根据namespace获取KV列表，NameServer处理该请求
     */
    public static final int GET_KVLIST_BY_NAMESPACE = 219;

    public static final int RESET_CONSUMER_CLIENT_OFFSET = 220;

    public static final int GET_CONSUMER_STATUS_FROM_CLIENT = 221;

    public static final int INVOKE_BROKER_TO_RESET_OFFSET = 222;

    public static final int INVOKE_BROKER_TO_GET_CONSUMER_STATUS = 223;

    public static final int QUERY_TOPIC_CONSUME_BY_WHO = 300;

    /**
     * 获取集群的Topic列表，NameServer处理该请求
     */
    public static final int GET_TOPICS_BY_CLUSTER = 224;

    public static final int REGISTER_FILTER_SERVER = 301;
    public static final int REGISTER_MESSAGE_FILTER_CLASS = 302;

    public static final int QUERY_CONSUME_TIME_SPAN = 303;

    /**
     * 获取系统的Topic列表，NameServer处理该请求
     */
    public static final int GET_SYSTEM_TOPIC_LIST_FROM_NS = 304;
    public static final int GET_SYSTEM_TOPIC_LIST_FROM_BROKER = 305;

    public static final int CLEAN_EXPIRED_CONSUMEQUEUE = 306;

    public static final int GET_CONSUMER_RUNNING_INFO = 307;

    public static final int QUERY_CORRECTION_OFFSET = 308;
    public static final int CONSUME_MESSAGE_DIRECTLY = 309;

    /**
     * 发送消息
     */
    public static final int SEND_MESSAGE_V2 = 310;

    /**
     * 获取一个单元中的所有Topic列表，有多个机房的情况下可以设置多个单元来避免跨机房调用，NameServer处理该请求
     */
    public static final int GET_UNIT_TOPIC_LIST = 311;

    /**
     * 获取子单元中所有Topic列表，NameServer处理该请求
     */
    public static final int GET_HAS_UNIT_SUB_TOPIC_LIST = 312;

    /**
     * 获取子单元中所有非单元的Topic列表，NameServer处理该请求
     */
    public static final int GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST = 313;

    public static final int CLONE_GROUP_OFFSET = 314;

    public static final int VIEW_BROKER_STATS_DATA = 315;

    public static final int CLEAN_UNUSED_TOPIC = 316;

    public static final int GET_BROKER_CONSUME_STATS = 317;

    /**
     * update the config of name server
     * 更新NameServer配置，NameServer处理该请求
     */
    public static final int UPDATE_NAMESRV_CONFIG = 318;

    /**
     * get config from name server
     * 获取NameServer配置，NameServer处理该请求
     */
    public static final int GET_NAMESRV_CONFIG = 319;

    public static final int SEND_BATCH_MESSAGE = 320;

    public static final int QUERY_CONSUME_QUEUE = 321;

    /**
     * 从brokerLiveTable中获取data version，并判断是否已经改变，NameServer处理该请求
     */
    public static final int QUERY_DATA_VERSION = 322;

    /**
     * resume logic of checking half messages that have been put in TRANS_CHECK_MAXTIME_TOPIC before
     */
    public static final int RESUME_CHECK_HALF_MESSAGE = 323;

    public static final int SEND_REPLY_MESSAGE = 324;

    public static final int SEND_REPLY_MESSAGE_V2 = 325;

    public static final int PUSH_REPLY_MESSAGE_TO_CLIENT = 326;
}
