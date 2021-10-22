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
package org.apache.rocketmq.remoting.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * RockeyMQ自己用来序列化Header Data的协议
 */
public class RocketMQSerializable {
    private static final Charset CHARSET_UTF8 = Charset.forName("UTF-8");

    /**
     * 按照RocketMQ自己的协议将Header Data进行序列化
     * @param cmd
     * @return
     */
    public static byte[] rocketMQProtocolEncode(RemotingCommand cmd) {
        // String remark
        // RemotingCommand的remark字段值的长度
        byte[] remarkBytes = null;
        int remarkLen = 0;
        if (cmd.getRemark() != null && cmd.getRemark().length() > 0) {
            remarkBytes = cmd.getRemark().getBytes(CHARSET_UTF8);
            remarkLen = remarkBytes.length;
        }

        // HashMap<String, String> extFields
        // RemotingCommand的extFields字段值序列化后的长度
        byte[] extFieldsBytes = null;
        int extLen = 0;
        if (cmd.getExtFields() != null && !cmd.getExtFields().isEmpty()) {
            // 将map序列化成字节数组
            extFieldsBytes = mapSerialize(cmd.getExtFields());
            extLen = extFieldsBytes.length;
        }

        // 计算Header Data的总长度
        int totalLen = calTotalLen(remarkLen, extLen);

        ByteBuffer headerBuffer = ByteBuffer.allocate(totalLen);
        // int code(~32767)
        // RemotingCommand的code字段
        headerBuffer.putShort((short) cmd.getCode());
        // LanguageCode language
        // RemotingCommand的language字段
        headerBuffer.put(cmd.getLanguage().getCode());
        // int version(~32767)
        // RemotingCommand的version字段
        headerBuffer.putShort((short) cmd.getVersion());
        // int opaque
        // RemotingCommand的opaque字段
        headerBuffer.putInt(cmd.getOpaque());
        // int flag
        // RemotingCommand的flag字段
        headerBuffer.putInt(cmd.getFlag());
        // String remark
        // RemotingCommand的remark字段
        if (remarkBytes != null) {
            headerBuffer.putInt(remarkBytes.length);
            headerBuffer.put(remarkBytes);
        } else {
            headerBuffer.putInt(0);
        }
        // HashMap<String, String> extFields;
        // RemotingCommand的extFields字段
        if (extFieldsBytes != null) {
            headerBuffer.putInt(extFieldsBytes.length);
            headerBuffer.put(extFieldsBytes);
        } else {
            headerBuffer.putInt(0);
        }

        return headerBuffer.array();
    }

    /**
     * 将map进行序列化，序列化成：keySize+key+valueSize+value这样的格式
     * @param map
     * @return
     */
    public static byte[] mapSerialize(HashMap<String, String> map) {
        // keySize+key+valSize+val
        if (null == map || map.isEmpty())
            return null;

        int totalLength = 0;
        int kvLength;
        Iterator<Map.Entry<String, String>> it = map.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, String> entry = it.next();
            if (entry.getKey() != null && entry.getValue() != null) {
                kvLength =
                    // keySize + Key
                    2 + entry.getKey().getBytes(CHARSET_UTF8).length
                        // valSize + val
                        + 4 + entry.getValue().getBytes(CHARSET_UTF8).length;
                totalLength += kvLength;
            }
        }

        ByteBuffer content = ByteBuffer.allocate(totalLength);
        byte[] key;
        byte[] val;
        it = map.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, String> entry = it.next();
            if (entry.getKey() != null && entry.getValue() != null) {
                key = entry.getKey().getBytes(CHARSET_UTF8);
                val = entry.getValue().getBytes(CHARSET_UTF8);

                content.putShort((short) key.length);
                content.put(key);

                content.putInt(val.length);
                content.put(val);
            }
        }

        return content.array();
    }

    /**
     * 计算Header Data字段的总长度
     * @param remark
     * @param ext
     * @return
     */
    private static int calTotalLen(int remark, int ext) {
        // int code(~32767)
        int length = 2 // RemotingCommand的code字段长度
            // LanguageCode language
            // RemotingCommand的language字段长度
            + 1
            // int version(~32767)
            // RemotingCommand的version字段长度
            + 2
            // int opaque
            // RemotingCommand的opaque字段长度
            + 4
            // int flag
            // RemotingCommand的flag字段长度
            + 4
            // String remark
            // 4是记录remark长度的字段的长度
            + 4
            // RemotingCommand的remark字段长度
            + remark
            // HashMap<String, String> extFields
            // 4是记录extFields长度的字段的长度
            + 4
            // RemotingCommand的extFields字段
            + ext;

        return length;
    }

    /**
     * 按照RocketMQ自己的协议将Header Data反序列化
     * @param headerArray
     * @return
     */
    public static RemotingCommand rocketMQProtocolDecode(final byte[] headerArray) {
        RemotingCommand cmd = new RemotingCommand();
        ByteBuffer headerBuffer = ByteBuffer.wrap(headerArray);
        // int code(~32767)
        // RemotingCommand的code字段
        cmd.setCode(headerBuffer.getShort());
        // LanguageCode language
        // RemotingCommand的language字段
        cmd.setLanguage(LanguageCode.valueOf(headerBuffer.get()));
        // int version(~32767)
        // RemotingCommand的version字段
        cmd.setVersion(headerBuffer.getShort());
        // int opaque
        // RemotingCommand的opaque字段
        cmd.setOpaque(headerBuffer.getInt());
        // int flag
        // RemotingCommand的flag字段
        cmd.setFlag(headerBuffer.getInt());
        // String remark
        // RemotingCommand的remark字段
        // remark字段的长度
        int remarkLength = headerBuffer.getInt();
        if (remarkLength > 0) {
            byte[] remarkContent = new byte[remarkLength];
            headerBuffer.get(remarkContent);
            cmd.setRemark(new String(remarkContent, CHARSET_UTF8));
        }

        // HashMap<String, String> extFields
        // RemotingCommand的extFields字段
        // extFields字段长度
        int extFieldsLength = headerBuffer.getInt();
        if (extFieldsLength > 0) {
            byte[] extFieldsBytes = new byte[extFieldsLength];
            headerBuffer.get(extFieldsBytes);
            // 需要将数据翻序列化成Map类型
            cmd.setExtFields(mapDeserialize(extFieldsBytes));
        }
        return cmd;
    }

    /**
     * 将extFields翻序列化成Map类型，
     * 序列化后的格式：keySize+key+valueSize+value，需要将这种格式转换成map类型
     * @param bytes
     * @return
     */
    public static HashMap<String, String> mapDeserialize(byte[] bytes) {
        if (bytes == null || bytes.length <= 0)
            return null;

        HashMap<String, String> map = new HashMap<String, String>();
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);

        short keySize;
        byte[] keyContent;
        int valSize;
        byte[] valContent;
        while (byteBuffer.hasRemaining()) {
            // key的长度
            keySize = byteBuffer.getShort();
            // key的内容
            keyContent = new byte[keySize];
            byteBuffer.get(keyContent);

            // value的长度
            valSize = byteBuffer.getInt();
            // value的内容
            valContent = new byte[valSize];
            byteBuffer.get(valContent);

            map.put(new String(keyContent, CHARSET_UTF8), new String(valContent, CHARSET_UTF8));
        }
        return map;
    }

    public static boolean isBlank(String str) {
        int strLen;
        if (str == null || (strLen = str.length()) == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if (!Character.isWhitespace(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }
}
