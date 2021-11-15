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

import com.alibaba.fastjson.annotation.JSONField;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

public class RemotingCommand {
    public static final String SERIALIZE_TYPE_PROPERTY = "rocketmq.serialize.type";
    public static final String SERIALIZE_TYPE_ENV = "ROCKETMQ_SERIALIZE_TYPE";
    public static final String REMOTING_VERSION_KEY = "rocketmq.remoting.version";
    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);
    private static final int RPC_TYPE = 0; // 0, REQUEST_COMMAND
    private static final int RPC_ONEWAY = 1; // 0, RPC
    private static final Map<Class<? extends CommandCustomHeader>, Field[]> CLASS_HASH_MAP =
        new HashMap<Class<? extends CommandCustomHeader>, Field[]>();
    private static final Map<Class, String> CANONICAL_NAME_CACHE = new HashMap<Class, String>();
    // 1, Oneway
    // 1, RESPONSE_COMMAND
    private static final Map<Field, Boolean> NULLABLE_FIELD_CACHE = new HashMap<Field, Boolean>();
    private static final String STRING_CANONICAL_NAME = String.class.getCanonicalName();
    private static final String DOUBLE_CANONICAL_NAME_1 = Double.class.getCanonicalName();
    private static final String DOUBLE_CANONICAL_NAME_2 = double.class.getCanonicalName();
    private static final String INTEGER_CANONICAL_NAME_1 = Integer.class.getCanonicalName();
    private static final String INTEGER_CANONICAL_NAME_2 = int.class.getCanonicalName();
    private static final String LONG_CANONICAL_NAME_1 = Long.class.getCanonicalName();
    private static final String LONG_CANONICAL_NAME_2 = long.class.getCanonicalName();
    private static final String BOOLEAN_CANONICAL_NAME_1 = Boolean.class.getCanonicalName();
    private static final String BOOLEAN_CANONICAL_NAME_2 = boolean.class.getCanonicalName();
    private static volatile int configVersion = -1;
    private static AtomicInteger requestId = new AtomicInteger(0);

    private static SerializeType serializeTypeConfigInThisServer = SerializeType.JSON;

    static {
        final String protocol = System.getProperty(SERIALIZE_TYPE_PROPERTY, System.getenv(SERIALIZE_TYPE_ENV));
        if (!isBlank(protocol)) {
            try {
                serializeTypeConfigInThisServer = SerializeType.valueOf(protocol);
            } catch (IllegalArgumentException e) {
                throw new RuntimeException("parser specified protocol error. protocol=" + protocol, e);
            }
        }
    }

    /**
     * Request:
     *  请求操作码，应答方根据不同的请求码进行不同的业务处理
     * Response:
     *  应答响应码，0表示成功，非0表示各种错误
     */
    private int code;

    /**
     * Request:
     *  请求方实现的语言
     * Response:
     *  应答方实现的语言
     */
    private LanguageCode language = LanguageCode.JAVA;

    /**
     * Request:
     *  请求方程序的版本
     * Response:
     *  应答方程序的版本
     */
    private int version = 0;

    /**
     * Request:
     *  相当于requestId，在同一个连接上的不同请求标识码，与响应消息中的相对应
     * Response:
     *  应答不做修改直接返回
     */
    private int opaque = requestId.getAndIncrement();

    /**
     * Request:
     *  区分是普通RPC还是onewayRPC的标志
     * Response:
     *  区分是普通RPC还是onewayRPC的标志
     */
    private int flag = 0;

    /**
     * Request:
     *  传输自定义文本信息
     * Response:
     *  传输自定义文本信息
     */
    private String remark;

    /**
     * Request:
     *  请求自定义扩展信息
     * Response:
     *  响应自定义扩展信息
     */
    private HashMap<String, String> extFields;

    /**
     * CustomHeader会被转换成extFields，key是字段的名字，value是字段的值
     */
    private transient CommandCustomHeader customHeader;

    /**
     * 用来序列化Header Data的序列化类型，有JSON和ROCKETMQ两种类型
     */
    private SerializeType serializeTypeCurrentRPC = serializeTypeConfigInThisServer;

    /**
     * Data Body，消息体
     */
    private transient byte[] body;

    protected RemotingCommand() {
    }

    /**
     * 创建请求命令
     * @param code
     * @param customHeader
     * @return
     */
    public static RemotingCommand createRequestCommand(int code, CommandCustomHeader customHeader) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.setCode(code);
        cmd.customHeader = customHeader;
        setCmdVersion(cmd);
        return cmd;
    }

    private static void setCmdVersion(RemotingCommand cmd) {
        if (configVersion >= 0) {
            cmd.setVersion(configVersion);
        } else {
            String v = System.getProperty(REMOTING_VERSION_KEY);
            if (v != null) {
                int value = Integer.parseInt(v);
                cmd.setVersion(value);
                configVersion = value;
            }
        }
    }

    /**
     * 创建响应命令
     * @param classHeader 自定义的Header，最终会被转换成extFields
     * @return
     */
    public static RemotingCommand createResponseCommand(Class<? extends CommandCustomHeader> classHeader) {
        return createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR, "not set any response code", classHeader);
    }

    public static RemotingCommand createResponseCommand(int code, String remark,
        Class<? extends CommandCustomHeader> classHeader) {
        RemotingCommand cmd = new RemotingCommand();
        // 响应类型：普通RPC还是单向RPC
        cmd.markResponseType();
        // 响应码
        cmd.setCode(code);
        // 自定义响应文本信息
        cmd.setRemark(remark);
        // 响应版本号
        setCmdVersion(cmd);

        // 实例化自定义Header
        if (classHeader != null) {
            try {
                CommandCustomHeader objectHeader = classHeader.newInstance();
                cmd.customHeader = objectHeader;
            } catch (InstantiationException e) {
                return null;
            } catch (IllegalAccessException e) {
                return null;
            }
        }

        return cmd;
    }

    public static RemotingCommand createResponseCommand(int code, String remark) {
        return createResponseCommand(code, remark, null);
    }

    public static RemotingCommand decode(final byte[] array) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(array);
        return decode(byteBuffer);
    }

    public static RemotingCommand decode(final ByteBuffer byteBuffer) {
        /*
            消息的整体编码格式如下：

            解码前的数据                       解码后的数据
            +--------+----------------+      +----------------+
            | Length | Actual Content |----->| Actual Content |
            +--------+----------------+      +----------------+

            RocketMQ实际编码格式如下：
            解码前                                                     解码后
            +--------+---------------+-------------+-----------+
            | 4Bytes |    4Bytes     |             |           |
            +--------+---------------+-------------+-----------+      +---------------+-------------+-----------+
            | Length | Header Length | Header Data | Body Data |----->| Header Length | Header Data | Body Data |
            +--------+---------------+-------------+-----------+      +---------------+-------------+-----------+

            其实到这里的byteBuffer已经是Netty解码后的数据了，已经不包含了最开始的Length域了，现在的数据如下：
            +---------------+-------------+-----------+
            | Header Length | Header Data | Body Data |
            +---------------+-------------+-----------+

            其中Header Length字段的前8位是存储Header Data的协议，后24位是Header Data的长度

         */
        // 接收到解码后的消息的总长度
        int length = byteBuffer.limit();

        // 消息最开始的4个字节是Header Length域，Header Length域中前8位是Header Data的协议，后24位是Header Data的长度
        int oriHeaderLen = byteBuffer.getInt();
        // Header Data的长度，存储在Header Length域的后24位
        int headerLength = getHeaderLength(oriHeaderLen);

        // Header Data
        byte[] headerData = new byte[headerLength];
        byteBuffer.get(headerData);

        // 根据具体的协议来解析HeaderData，协议存储在Header Length的前8位
        RemotingCommand cmd = headerDecode(headerData, getProtocolType(oriHeaderLen));

        // 消息主体数据Body Data，length是解码后的消息总体长度，4是最开始的Header Length的长度，headerLength是header Data的长度
        int bodyLength = length - 4 - headerLength;
        byte[] bodyData = null;
        if (bodyLength > 0) {
            bodyData = new byte[bodyLength];
            byteBuffer.get(bodyData);
        }
        cmd.body = bodyData;

        return cmd;
    }

    public static int getHeaderLength(int length) {
        return length & 0xFFFFFF;
    }

    private static RemotingCommand headerDecode(byte[] headerData, SerializeType type) {
        switch (type) {
            case JSON:
                RemotingCommand resultJson = RemotingSerializable.decode(headerData, RemotingCommand.class);
                resultJson.setSerializeTypeCurrentRPC(type);
                return resultJson;
            case ROCKETMQ:
                RemotingCommand resultRMQ = RocketMQSerializable.rocketMQProtocolDecode(headerData);
                resultRMQ.setSerializeTypeCurrentRPC(type);
                return resultRMQ;
            default:
                break;
        }

        return null;
    }

    /**
     * 获取Header Data的协议类型
     * Header Length前8位是协议类型，后24位是Header Data的长度
     * @param source
     * @return
     */
    public static SerializeType getProtocolType(int source) {
        return SerializeType.valueOf((byte) ((source >> 24) & 0xFF));
    }

    public static int createNewRequestId() {
        return requestId.getAndIncrement();
    }

    public static SerializeType getSerializeTypeConfigInThisServer() {
        return serializeTypeConfigInThisServer;
    }

    private static boolean isBlank(String str) {
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

    /**
     * 将序列化的类型和头部长度编码放到一个byte[4]数组中
     * @param source
     * @param type
     * @return
     */
    public static byte[] markProtocolType(int source, SerializeType type) {
        byte[] result = new byte[4];

        result[0] = type.getCode();
        // 右移16位后再和255与->“16-24位”
        result[1] = (byte) ((source >> 16) & 0xFF);
        // 右移8位后再和255与->“8-16位”
        result[2] = (byte) ((source >> 8) & 0xFF);
        // 右移0位后再和255与->“8-0位”
        result[3] = (byte) (source & 0xFF);
        return result;
    }

    public void markResponseType() {
        int bits = 1 << RPC_TYPE;
        this.flag |= bits;
    }

    public CommandCustomHeader readCustomHeader() {
        return customHeader;
    }

    public void writeCustomHeader(CommandCustomHeader customHeader) {
        this.customHeader = customHeader;
    }

    /**
     * 从extFields中还原自定义Header
     * @param classHeader
     * @return
     * @throws RemotingCommandException
     */
    public CommandCustomHeader decodeCommandCustomHeader(
        Class<? extends CommandCustomHeader> classHeader) throws RemotingCommandException {
        CommandCustomHeader objectHeader;
        try {
            // 实例化自定义Header
            objectHeader = classHeader.newInstance();
        } catch (InstantiationException e) {
            return null;
        } catch (IllegalAccessException e) {
            return null;
        }

        // 从extFields中还原
        if (this.extFields != null) {

            Field[] fields = getClazzFields(classHeader);
            for (Field field : fields) {
                if (!Modifier.isStatic(field.getModifiers())) {
                    String fieldName = field.getName();
                    if (!fieldName.startsWith("this")) {
                        try {
                            String value = this.extFields.get(fieldName);
                            if (null == value) {
                                if (!isFieldNullable(field)) {
                                    throw new RemotingCommandException("the custom field <" + fieldName + "> is null");
                                }
                                continue;
                            }

                            field.setAccessible(true);
                            String type = getCanonicalName(field.getType());
                            Object valueParsed;

                            if (type.equals(STRING_CANONICAL_NAME)) {
                                valueParsed = value;
                            } else if (type.equals(INTEGER_CANONICAL_NAME_1) || type.equals(INTEGER_CANONICAL_NAME_2)) {
                                valueParsed = Integer.parseInt(value);
                            } else if (type.equals(LONG_CANONICAL_NAME_1) || type.equals(LONG_CANONICAL_NAME_2)) {
                                valueParsed = Long.parseLong(value);
                            } else if (type.equals(BOOLEAN_CANONICAL_NAME_1) || type.equals(BOOLEAN_CANONICAL_NAME_2)) {
                                valueParsed = Boolean.parseBoolean(value);
                            } else if (type.equals(DOUBLE_CANONICAL_NAME_1) || type.equals(DOUBLE_CANONICAL_NAME_2)) {
                                valueParsed = Double.parseDouble(value);
                            } else {
                                throw new RemotingCommandException("the custom field <" + fieldName + "> type is not supported");
                            }

                            field.set(objectHeader, valueParsed);

                        } catch (Throwable e) {
                            log.error("Failed field [{}] decoding", fieldName, e);
                        }
                    }
                }
            }

            objectHeader.checkFields();
        }

        return objectHeader;
    }

    private Field[] getClazzFields(Class<? extends CommandCustomHeader> classHeader) {
        Field[] field = CLASS_HASH_MAP.get(classHeader);

        if (field == null) {
            field = classHeader.getDeclaredFields();
            synchronized (CLASS_HASH_MAP) {
                CLASS_HASH_MAP.put(classHeader, field);
            }
        }
        return field;
    }

    private boolean isFieldNullable(Field field) {
        if (!NULLABLE_FIELD_CACHE.containsKey(field)) {
            Annotation annotation = field.getAnnotation(CFNotNull.class);
            synchronized (NULLABLE_FIELD_CACHE) {
                NULLABLE_FIELD_CACHE.put(field, annotation == null);
            }
        }
        return NULLABLE_FIELD_CACHE.get(field);
    }

    private String getCanonicalName(Class clazz) {
        String name = CANONICAL_NAME_CACHE.get(clazz);

        if (name == null) {
            name = clazz.getCanonicalName();
            synchronized (CANONICAL_NAME_CACHE) {
                CANONICAL_NAME_CACHE.put(clazz, name);
            }
        }
        return name;
    }

    /**
     * 协议格式：
     *
     * -----------------------------------------------------------------
     * |  消息长度  |  序列化类型&&头部长度  |  消息头部数据  | 消息主体数据  |
     * -----------------------------------------------------------------
     *
     * 消息长度：总长度，int类型，用4个字节存储
     * 序列化类型&&头部长度：int类型，第一个字节表示序列化类型，后面三个字节表示消息头长度
     * 消息头部数据：经过序列化后的消息头数据
     * 消息主体数据：消息主体的二进制字节数据内容
     *
     * @return
     */
    public ByteBuffer encode() {

        /*
            消息的整体编码格式如下：

            解码前的数据                       解码后的数据
            +--------+----------------+      +----------------+
            | Length | Actual Content |----->| Actual Content |
            +--------+----------------+      +----------------+

            RocketMQ实际编码格式如下：
            解码前                                                     解码后
            +--------+---------------+-------------+-----------+
            | 4Bytes |    4Bytes     |             |           |
            +--------+---------------+-------------+-----------+      +---------------+-------------+-----------+
            | Length | Header Length | Header Data | Body Data |----->| Header Length | Header Data | Body Data |
            +--------+---------------+-------------+-----------+      +---------------+-------------+-----------+

            其中Header Length字段的前8位是存储Header Data的协议，后24位是Header Data的长度

         */
        // 1> header length size
        // Header Length的长度4个字节
        int length = 4;

        // 2> header data length
        // 将Header Data进行序列化
        byte[] headerData = this.headerEncode();
        // 4 + Header Length + Header Data
        length += headerData.length;

        // 3> body data length
        // Body Data的大小
        if (this.body != null) {
            // 4 + Header Length + Header Data + Body Data
            length += body.length;
        }

        // 分配ByteBuffer
        // 4 + Header Length + Header Data + Body Data + 4
        // 这里再次加4是要把长度域Length的长度也加进去
        ByteBuffer result = ByteBuffer.allocate(4 + length);

        // length
        // 消息总长度放入前面4个字节，也就是长度域Length
        result.putInt(length);

        // header length
        // Header Length数据
        result.put(markProtocolType(headerData.length, serializeTypeCurrentRPC));

        // header data
        // Header Data数据
        result.put(headerData);

        // body data;
        // Body Data数据
        if (this.body != null) {
            result.put(this.body);
        }

        // 重置ByteBuffer的position位置
        result.flip();

        return result;
    }

    private byte[] headerEncode() {
        this.makeCustomHeaderToNet();
        if (SerializeType.ROCKETMQ == serializeTypeCurrentRPC) {
            return RocketMQSerializable.rocketMQProtocolEncode(this);
        } else {
            return RemotingSerializable.encode(this);
        }
    }

    public void makeCustomHeaderToNet() {
        if (this.customHeader != null) {
            Field[] fields = getClazzFields(customHeader.getClass());
            if (null == this.extFields) {
                this.extFields = new HashMap<String, String>();
            }

            for (Field field : fields) {
                if (!Modifier.isStatic(field.getModifiers())) {
                    String name = field.getName();
                    if (!name.startsWith("this")) {
                        Object value = null;
                        try {
                            field.setAccessible(true);
                            value = field.get(this.customHeader);
                        } catch (Exception e) {
                            log.error("Failed to access field [{}]", name, e);
                        }

                        if (value != null) {
                            this.extFields.put(name, value.toString());
                        }
                    }
                }
            }
        }
    }

    public ByteBuffer encodeHeader() {
        return encodeHeader(this.body != null ? this.body.length : 0);
    }

    /**
     * 编码Header，不包含Body
     * @param bodyLength
     * @return
     */
    public ByteBuffer encodeHeader(final int bodyLength) {
        /*
            消息的整体编码格式如下：

            解码前的数据                       解码后的数据
            +--------+----------------+      +----------------+
            | Length | Actual Content |----->| Actual Content |
            +--------+----------------+      +----------------+

            RocketMQ实际编码格式如下：
            解码前                                                     解码后
            +--------+---------------+-------------+-----------+
            | 4Bytes |    4Bytes     |             |           |
            +--------+---------------+-------------+-----------+      +---------------+-------------+-----------+
            | Length | Header Length | Header Data | Body Data |----->| Header Length | Header Data | Body Data |
            +--------+---------------+-------------+-----------+      +---------------+-------------+-----------+

            其中Header Length字段的前8位是存储Header Data的协议，后24位是Header Data的长度

         */
        // 1> header length size
        // Header Length的长度4个字节
        int length = 4;

        // 2> header data length
        // Header Data序列化后的长度
        byte[] headerData;
        headerData = this.headerEncode();

        // Header Length + Header Data
        length += headerData.length;

        // 3> body data length
        // Header Length + Header Data + Body Data
        length += bodyLength;

        /*
            4 + length - bodyLength
            这里的4是Length域的长度是4字节

            这里result是用来存储：Length + Header Length + Header Data的，没有Body Data。
         */
        ByteBuffer result = ByteBuffer.allocate(4 + length - bodyLength);

        // length
        // Length域，用来存储消息长度，这里消息长度不包含Length域本身长度，只有Header Length + Header Data + Body Data的长度
        result.putInt(length);

        // header length
        // Header Length，前8位是序列化方式，后24位是Header Data的真实长度
        result.put(markProtocolType(headerData.length, serializeTypeCurrentRPC));

        // header data
        // Header Data数据
        result.put(headerData);

        result.flip();

        return result;
    }

    public void markOnewayRPC() {
        int bits = 1 << RPC_ONEWAY;
        this.flag |= bits;
    }

    @JSONField(serialize = false)
    public boolean isOnewayRPC() {
        int bits = 1 << RPC_ONEWAY;
        return (this.flag & bits) == bits;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    @JSONField(serialize = false)
    public RemotingCommandType getType() {
        if (this.isResponseType()) {
            return RemotingCommandType.RESPONSE_COMMAND;
        }

        return RemotingCommandType.REQUEST_COMMAND;
    }

    @JSONField(serialize = false)
    public boolean isResponseType() {
        int bits = 1 << RPC_TYPE;
        return (this.flag & bits) == bits;
    }

    public LanguageCode getLanguage() {
        return language;
    }

    public void setLanguage(LanguageCode language) {
        this.language = language;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getOpaque() {
        return opaque;
    }

    public void setOpaque(int opaque) {
        this.opaque = opaque;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public HashMap<String, String> getExtFields() {
        return extFields;
    }

    public void setExtFields(HashMap<String, String> extFields) {
        this.extFields = extFields;
    }

    public void addExtField(String key, String value) {
        if (null == extFields) {
            extFields = new HashMap<String, String>();
        }
        extFields.put(key, value);
    }

    @Override
    public String toString() {
        return "RemotingCommand [code=" + code + ", language=" + language + ", version=" + version + ", opaque=" + opaque + ", flag(B)="
            + Integer.toBinaryString(flag) + ", remark=" + remark + ", extFields=" + extFields + ", serializeTypeCurrentRPC="
            + serializeTypeCurrentRPC + "]";
    }

    public SerializeType getSerializeTypeCurrentRPC() {
        return serializeTypeCurrentRPC;
    }

    public void setSerializeTypeCurrentRPC(SerializeType serializeTypeCurrentRPC) {
        this.serializeTypeCurrentRPC = serializeTypeCurrentRPC;
    }
}