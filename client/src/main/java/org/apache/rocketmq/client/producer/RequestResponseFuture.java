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

package org.apache.rocketmq.client.producer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.common.message.Message;

public class RequestResponseFuture {
    private final String requestUniqId;
    private final RequestCallback requestCallback;
    private final long beginTimestamp = System.currentTimeMillis();
    private final Message requestMsg = null;
    private long timeoutMillis;
    private CountDownLatch countDownLatch = new CountDownLatch(1);
    private AtomicBoolean ececuteCallbackOnlyOnce = new AtomicBoolean(false);
    private volatile Message responseMsg = null;
    private volatile boolean sendReqeustOk = true;
    private volatile Throwable cause = null;

    public RequestResponseFuture(String requestUniqId, long timeoutMillis, RequestCallback requestCallback) {
        this.requestUniqId = requestUniqId;
        this.timeoutMillis = timeoutMillis;
        this.requestCallback = requestCallback;
    }

    public void executeRequestCallback() {
        if (requestCallback != null) {
            if (sendReqeustOk && cause == null) {
                requestCallback.onSuccess(responseMsg);
            } else {
                requestCallback.onException(cause);
            }
        }
    }

    public boolean isTimeout() {
        long diff = System.currentTimeMillis() - this.beginTimestamp;
        return diff > this.timeoutMillis;
    }

    public Message waitResponseMessage(final long timeout) throws InterruptedException {
        this.countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
        return this.responseMsg;
    }

    public void putResponseMessage(final Message responseMsg) {
        this.responseMsg = responseMsg;
        this.countDownLatch.countDown();
    }

    public String getRequestUniqId() {
        return requestUniqId;
    }

    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    public void setTimeoutMillis(long timeoutMillis) {
        this.timeoutMillis = timeoutMillis;
    }

    public RequestCallback getRequestCallback() {
        return requestCallback;
    }

    public long getBeginTimestamp() {
        return beginTimestamp;
    }

    public CountDownLatch getCountDownLatch() {
        return countDownLatch;
    }

    public void setCountDownLatch(CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
    }

    public AtomicBoolean getEcecuteCallbackOnlyOnce() {
        return ececuteCallbackOnlyOnce;
    }

    public void setEcecuteCallbackOnlyOnce(AtomicBoolean ececuteCallbackOnlyOnce) {
        this.ececuteCallbackOnlyOnce = ececuteCallbackOnlyOnce;
    }

    public Message getResponseMsg() {
        return responseMsg;
    }

    public void setResponseMsg(Message responseMsg) {
        this.responseMsg = responseMsg;
    }

    public boolean isSendReqeustOk() {
        return sendReqeustOk;
    }

    public void setSendReqeustOk(boolean sendReqeustOk) {
        this.sendReqeustOk = sendReqeustOk;
    }

    public Message getRequestMsg() {
        return requestMsg;
    }

    public Throwable getCause() {
        return cause;
    }

    public void setCause(Throwable cause) {
        this.cause = cause;
    }
}
