package com.blr.common.rabbit.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/**
 * 默认的回调和监听器
 * confirm消息发送到RabbitMQ交换器后接收ack回调
 * returnedMessage消息发送到RabbitMQ交换器,但无相应队列与交换器绑定时的回调
 * ChannelAwareMessageListener 消息监听
 */
@Slf4j
public class CallbackDefault implements RabbitTemplate.ConfirmCallback, RabbitTemplate.ReturnCallback {
    @Override
    public void confirm(CorrelationData correlationData, boolean ack, String cause) {
    }

    @Override
    public void returnedMessage(Message message, int replyCode, String replyText, String exchange, String routingKey) {
        log.error("RabbitMQ发送消息失败[信息:{},id:{},原因:{},交换机:{},绑定名:{}]", new String(message.getBody()), replyCode, replyText, exchange, replyCode);
    }
}
