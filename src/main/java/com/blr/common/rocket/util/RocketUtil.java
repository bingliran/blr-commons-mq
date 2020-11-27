package com.blr.common.rocket.util;

import com.alibaba.fastjson.JSONObject;
import com.blr.common.rocket.config.RocketInfo;
import com.blr.common.rocket.exception.RocketSendMessageException;
import com.blr.common.auto.transport.RequestData;
import com.blr.common.rocket.config.RocketInfo;
import com.blr.common.rocket.exception.RocketSendMessageException;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import javax.annotation.PreDestroy;

/**
 * rocket的便捷方法
 */
public class RocketUtil {

    @Autowired
    protected ApplicationContext applicationContext;

    /**
     * 发送信息并且获取结果
     */
    public SendResult sendMessage(RequestData<?> requestData, AbstractProducer producer) {
        try {
            return producer.send(getMessage(producer.getEntity(), requestData));
        } catch (Exception e) {
            throw new RocketSendMessageException("发送失败:" + e.getMessage(), e);
        }
    }

    /**
     * 发送一个异步处理的消息
     * SendCallback用于成功或失败执行
     */
    public void sendMessage(RequestData<?> requestData, AbstractProducer producer, SendCallback sendCallback) {
        try {
            producer.send(getMessage(producer.getEntity(), requestData), sendCallback);
        } catch (Exception e) {
            throw new RocketSendMessageException("发送失败:" + e.getMessage(), e);
        }
    }

    /**
     * 发送一个单向消息,发送即成功不关注结果
     */
    public void sendOneWayMessage(RequestData<?> requestData, AbstractProducer producer) {
        try {
            producer.sendOneway(getMessage(producer.getEntity(), requestData));
        } catch (Exception e) {
            throw new RocketSendMessageException("发送失败:" + e.getMessage(), e);
        }
    }

    /**
     * 发送一个延时消息,int level为延时消息的等级
     * 延时消息在broker配置,无法使用程序修改
     * 默认值为messageDelayLevel=1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
     */
    public void sendDelayTimeMessage(RequestData<?> requestData, int level, AbstractProducer producer) {
        try {
            Message message = getMessage(producer.getEntity(), requestData);
            message.setDelayTimeLevel(level);
            producer.send(message);
        } catch (Exception e) {
            throw new RocketSendMessageException("发送失败:" + e.getMessage(), e);
        }
    }

    /**
     * 发送一个使用事务的信息并获取事务结果
     */
    public TransactionSendResult sendTransactionMessage(RequestData<?> requestData, AbstractProducer producer, Object arg) {
        try {
            return producer.sendMessageInTransaction(getMessage(producer.getEntity(), requestData), arg);
        } catch (Exception e) {
            throw new RocketSendMessageException("发送失败:" + e.getMessage(), e);
        }
    }

    /**
     * 如果是使用的默认producer
     */
    public SendResult sendMessage(RequestData<?> requestData) {
        return sendMessage(requestData, getDefaultProducer());
    }

    public void sendMessage(RequestData<?> requestData, SendCallback sendCallback) {
        sendMessage(requestData, getDefaultProducer(), sendCallback);
    }

    public TransactionSendResult sendTransactionMessage(RequestData<?> requestData, Object arg) {
        return sendTransactionMessage(requestData, getDefaultProducer(), arg);
    }

    public void sendOneWayMessage(RequestData<?> requestData) {
        sendOneWayMessage(requestData, getDefaultProducer());
    }

    public void sendDelayTimeMessage(RequestData<?> requestData, int level) {
        sendDelayTimeMessage(requestData, level, getDefaultProducer());
    }

    /**
     * 通过AbstractProducer支持的entity获取发送信息
     */
    public Message getMessage(RocketInfo.Entity entity, RequestData<?> requestData) {
        if (requestData.getTopic() == null) requestData.setTopic(entity.getTopic());
        Message message = new Message(entity.getTopic(), entity.getTag(), JSONObject.toJSONString(requestData).getBytes());
        if (requestData.getTag() != null) message.setTags(requestData.getTag());
        if (requestData.getKey() != null) message.setKeys(requestData.getKey());
        return message;
    }

    /**
     * 创建一个producer对象
     */
    public AbstractProducer generateProducer(RocketInfo rocketInfo) {
        return generateProducer(rocketInfo.getEntity());
    }

    public AbstractProducer generateProducer(RocketInfo.Entity entity) {
        return generateProducer(entity, null);
    }

    /**
     * 未创建TransactionListener的producer无法使用 sendTransactionMessage
     */
    public AbstractProducer generateProducer(RocketInfo.Entity entity, TransactionListener transactionListener) {
        return AbstractProducer.generateProducer(entity, transactionListener);
    }

    @PreDestroy
    public void close() {
        AbstractProducer.shutdownAll();
    }

    public void close(String id) {
        AbstractProducer.shutdown(id);
    }

    /**
     * 获取adminUtil以避免出现@Autowired出现null
     */
    public RocketAdminUtil getAdminUtil() {
        return applicationContext.getBean("rocketAdminUtil", RocketAdminUtil.class);
    }

    public AbstractProducer getDefaultProducer() {
        return applicationContext.getBean("defaultProducer", AbstractProducer.class);
    }

}
