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

package org.apache.rocketmq.common.protocol.header.namesrv;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNullable;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

/**
 * 注册Broker到NameServer请求对应的响应信息的Header
 */
public class RegisterBrokerResponseHeader implements CommandCustomHeader {

    /**
     * 高可用地址，存在主从broker时，如果在broker主节点上配置了brokerIP2属性，broker从节点会连接主节点配置的brokerIP2进行同步
     * 如果是从Broker注册，则该值是Master的BrokerIP2的值
     */
    @CFNullable
    private String haServerAddr;

    /**
     * 主Broker的地址
     */
    @CFNullable
    private String masterAddr;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public String getHaServerAddr() {
        return haServerAddr;
    }

    public void setHaServerAddr(String haServerAddr) {
        this.haServerAddr = haServerAddr;
    }

    public String getMasterAddr() {
        return masterAddr;
    }

    public void setMasterAddr(String masterAddr) {
        this.masterAddr = masterAddr;
    }
}
