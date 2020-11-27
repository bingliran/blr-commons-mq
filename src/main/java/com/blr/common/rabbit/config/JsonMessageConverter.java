package com.blr.common.rabbit.config;

import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.MapUtils;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.Nullable;
import org.springframework.util.StringUtils;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;

/**
 * 重写MessageConverter实现自定义实体类并且位置无需相同,以及不需要实现序列化接口
 */
public class JsonMessageConverter extends Jackson2JsonMessageConverter {
    @Autowired
    private RabbitInfo rabbitInfo;

    public JsonMessageConverter(ObjectMapper objectMapper) {
        super(objectMapper);
    }

    public JsonMessageConverter() {
        super();
    }

    @Override
    protected Message createMessage(Object objectToConvert, MessageProperties messageProperties, @Nullable Type genericType)
            throws MessageConversionException {
        if (MapUtils.getBoolean(messageProperties.getHeaders(), RabbitInfo.IS_JMC)) {
            byte[] data = new byte[0];
            try {
                if (objectToConvert != null)
                    data = JSON.toJSONString(objectToConvert).getBytes(rabbitInfo.getEncoding());
            } catch (UnsupportedEncodingException e) {
                throw new MessageConversionException("转换消息内容失败", e);
            }
            messageProperties.setContentType("application/json");
            messageProperties.setHeader("classNameMark", objectToConvert.getClass().getSimpleName());
            messageProperties.setHeader("classPathMark", objectToConvert.getClass().getCanonicalName());
            messageProperties.setContentEncoding(rabbitInfo.getEncoding());
            messageProperties.setContentLength(data.length);
            if (getClassMapper() == null) {
                getJavaTypeMapper().fromJavaType(this.objectMapper.constructType(
                        genericType == null ? objectToConvert.getClass() : genericType), messageProperties);
            } else getClassMapper().fromClass(objectToConvert.getClass(), messageProperties);
            return new Message(data, messageProperties);
        } else return super.createMessage(objectToConvert, messageProperties, genericType);
    }

    @Override
    public Object fromMessage(Message message, @Nullable Object conversionHint) throws MessageConversionException {
        MessageProperties properties = message.getMessageProperties();
        if (properties == null) return super.fromMessage(message, conversionHint);
        String classNameMark = MapUtils.getString(properties.getHeaders(), "classNameMark");
        String classPathMark = MapUtils.getString(properties.getHeaders(), "classPathMark");
        /**
         * 如果缺少任何要素信息则不使用重写方法
         */
        if (!MapUtils.getBoolean(properties.getHeaders(), RabbitInfo.IS_JMC) || StringUtils.isEmpty(classPathMark) || StringUtils.isEmpty(classNameMark) || rabbitInfo.getClassNamePathMark() == null)
            return super.fromMessage(message, conversionHint);
        if ("application/json".equals(properties.getContentType())) {
            try {
                /**
                 * 验证字符集,如果使用的是super.fromMessage那么会直接出现MessageConversionException
                 */
                String encoding = properties.getContentEncoding();
                if (encoding != null && !rabbitInfo.getEncoding().equals(encoding)) {
                    log.warn("本地字符集" + rabbitInfo.getEncoding() + "与发送者字符集不一致" + encoding);
                }
                String convertString = new String(message.getBody(), encoding);
                /**
                 * 根据给定的路径和默认的路径查找实例
                 */
                Class<?> cls;
                try {
                    cls = Class.forName(classPathMark);
                } catch (ClassNotFoundException e) {
                    String classPath = rabbitInfo.getClassNamePathMark() + "." + classNameMark;
                    cls = Class.forName(classPath);
                }
                Object returnObj = JSON.parseObject(convertString, cls);
                return returnObj == null ? message.getBody() : returnObj;
            } catch (Exception e) {
                throw new MessageConversionException("转换消息内容失败", e);
            }
        } else return super.fromMessage(message, conversionHint);
    }
}
