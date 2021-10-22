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
package org.apache.rocketmq.remoting.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import java.nio.ByteBuffer;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/*
    NettyDecoder基于Netty的LengthFieldBasedFrameDecoder实现，从NettyDecode的构造方法可以
    推断出RocketMQ的基本编码的格式以及解码后的格式，构造方法的一些参数如下：
    - lengthFieldOffset = 0，说明消息的最开始的字节就是长度域
    - lengthFieldLength=4，说明长度域本身长度是4个字节
    - lengthAdjustment=0，说明长度域中的长度只包含实际数据的长度，不包含长度域本身的长度，不需要修正
    - initialBytesToStrip=4，说明要跳过最开始的长度域4个字节，也就是解码后的数据不包含长度域，只包含数据
    由此可推断基本编码以及解码后的个数如下：
    解码前的数据                       解码后的数据
    +--------+----------------+      +----------------+
    | Length | Actual Content |----->| Actual Content |
    +--------+----------------+      +----------------+

    示例：
    解码前的数据长度14个字节             解码后的数据长度12个字节
    +--------+----------------+      +----------------+
    | 4Bytes |    12Bytes     |----->|    12Bytes     |
    +--------+----------------+      +----------------+
    | Length | Actual Content |----->| Actual Content |
    |   12   | "HELLO, WORLD" |      | "HELLO, WORLD" |
    +--------+----------------+      +----------------+
 */
public class NettyDecoder extends LengthFieldBasedFrameDecoder {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    private static final int FRAME_MAX_LENGTH =
        Integer.parseInt(System.getProperty("com.rocketmq.remoting.frameMaxLength", "16777216"));

    public NettyDecoder() {
        super(FRAME_MAX_LENGTH, 0, 4, 0, 4);
    }

    @Override
    public Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        ByteBuf frame = null;
        try {
            // 经过这一步LengthFieldBasedFrameDecoder的decode后，就拿到了解码后的数据，已经不包含了最开始的4字节的长度域
            frame = (ByteBuf) super.decode(ctx, in);
            if (null == frame) {
                return null;
            }

            ByteBuffer byteBuffer = frame.nioBuffer();

            // decode中是具体的解码过程
            return RemotingCommand.decode(byteBuffer);
        } catch (Exception e) {
            log.error("decode exception, " + RemotingHelper.parseChannelRemoteAddr(ctx.channel()), e);
            RemotingUtil.closeChannel(ctx.channel());
        } finally {
            if (null != frame) {
                frame.release();
            }
        }

        return null;
    }
}
