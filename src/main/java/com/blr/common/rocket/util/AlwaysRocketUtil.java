package com.blr.common.rocket.util;

import com.blr.common.auto.transport.RequestData;
import com.blr.common.rocket.exception.RocketSendMessageException;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;

/**
 * 重写RocketUtil的所有发送方法提供一个每次发送都会新建一个producer的实现
 */
public class AlwaysRocketUtil extends RocketUtil {

    /**
     * 发送信息并且获取结果
     */
    @Override
    public SendResult sendMessage(RequestData<?> requestData, AbstractProducer abstractProducer) {
        try (AbstractProducer producer = generateProducer(abstractProducer.getEntity())) {
            abstractProducer.close();
            producer.start();
            return producer.send(getMessage(producer.getEntity(), requestData));
        } catch (Exception e) {
            throw new RocketSendMessageException("发送失败:" + e.getMessage(), e);
        }
    }

    /**
     * 发送一个异步处理的消息
     * SendCallback用于成功或失败执行
     */
    @Override
    public void sendMessage(RequestData<?> requestData, AbstractProducer abstractProducer, SendCallback sendCallback) {
        try (AbstractProducer producer = generateProducer(abstractProducer.getEntity())) {
            abstractProducer.close();
            producer.start();
            producer.send(getMessage(producer.getEntity(), requestData), sendCallback);
        } catch (Exception e) {
            throw new RocketSendMessageException("发送失败:" + e.getMessage(), e);
        }
    }

    /**
     * 发送一个单向消息,发送即成功不关注结果
     */
    @Override
    public void sendOneWayMessage(RequestData<?> requestData, AbstractProducer abstractProducer) {
        try (AbstractProducer producer = generateProducer(abstractProducer.getEntity())) {
            abstractProducer.close();
            producer.start();
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
    @Override
    public void sendDelayTimeMessage(RequestData<?> requestData, int level, AbstractProducer abstractProducer) {
        try (AbstractProducer producer = generateProducer(abstractProducer.getEntity())) {
            abstractProducer.close();
            producer.start();
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
    @Override
    public TransactionSendResult sendTransactionMessage(RequestData<?> requestData, AbstractProducer abstractProducer, Object arg) {
        try (AbstractProducer producer = generateProducer(abstractProducer.getEntity())) {
            abstractProducer.close();
            producer.start();
            return producer.sendMessageInTransaction(getMessage(producer.getEntity(), requestData), arg);
        } catch (Exception e) {
            throw new RocketSendMessageException("发送失败:" + e.getMessage(), e);
        }
    }

    /**
     * 如果是使用的默认producer
     */
    @Override
    public SendResult sendMessage(RequestData<?> requestData) {
        return sendMessage(requestData, getDefaultProducer());
    }

    @Override
    public void sendMessage(RequestData<?> requestData, SendCallback sendCallback) {
        sendMessage(requestData, getDefaultProducer(), sendCallback);
    }

    @Override
    public TransactionSendResult sendTransactionMessage(RequestData<?> requestData, Object arg) {
        return sendTransactionMessage(requestData, getDefaultProducer(), arg);
    }

    @Override
    public void sendOneWayMessage(RequestData<?> requestData) {
        sendOneWayMessage(requestData, getDefaultProducer());
    }

    @Override
    public void sendDelayTimeMessage(RequestData<?> requestData, int level) {
        sendDelayTimeMessage(requestData, level, getDefaultProducer());
    }
}
