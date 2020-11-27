package com.blr.common.rabbit.config;

import com.blr.common.auto.util.EnvironmentUtil;
import com.blr.common.rabbit.util.RabbitUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.retry.RetryContext;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.io.IOException;

/**
 * 提供RabbitMQ的配置
 */
@Slf4j
public class RabbitConfig {
    @Autowired
    private RabbitInfo rabbitInfo;
    @Autowired
    private Jackson2JsonMessageConverter JsonMessageConverter;

    @Bean
    public RabbitInfo rabbitInfo(EnvironmentUtil environment) {
        return new RabbitInfo(environment);
    }

    @Bean
    public CachingConnectionFactory cachingConnectionFactory() {
        CachingConnectionFactoryDowngrade downgrade = new CachingConnectionFactoryDowngrade();
        downgrade.setUsername(rabbitInfo.getUsername());
        downgrade.setPassword(rabbitInfo.getPassword());
        downgrade.setPort(rabbitInfo.getPort());
        downgrade.setHost(rabbitInfo.getHost());
        return downgrade;
    }

    /**
     * 提供javaBean和json互相转换的MessageConverter实现
     */
    @Bean
    public Jackson2JsonMessageConverter JsonMessageConverter() {
        ObjectMapper objectMapper = new ObjectMapper().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        return new JsonMessageConverter(objectMapper);
    }

    @Bean
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(CachingConnectionFactory connectionFactory) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setMessageConverter(JsonMessageConverter);
        /*
         * 如果非事务则设置确认模式
         */
        factory.setChannelTransacted(rabbitInfo.isTransactional());
        if (!rabbitInfo.isTransactional()) factory.setAcknowledgeMode(rabbitInfo.getAcknowledgeMode());
        /*
         *设置为循环调度:
         * 0总是以空闲的优先
         * 1为公平调度:一个服务一次
         */
        factory.setPrefetchCount(rabbitInfo.getPrefetchCount());
        /*
         * false:当收到消息reject请求直接丢弃消息
         */
        factory.setDefaultRequeueRejected(false);
        /*
         * 异常处理
         */
        factory.setErrorHandler(new UserCauseExceptionStrategy());
        return factory;
    }

    /**
     * 一个封装了rabbit基本操作的工具
     */
    @Bean
    public RabbitAdmin rabbitAdmin(CachingConnectionFactory connectionFactory) {
        return new RabbitAdmin(connectionFactory);
    }

    /**
     * 可以在引用此包的项目中定义这两个bean来覆盖defaultCallbackBean
     * 注:替换类的@Component注解中value/name的值必须为 returnCallbackReplace/confirmCallbackReplace
     */
    @Autowired(required = false)
    private RabbitTemplate.ReturnCallback returnCallbackReplace;

    @Autowired(required = false)
    private RabbitTemplate.ConfirmCallback confirmCallbackReplace;

    @Bean
    public RabbitTemplate rabbitTemplate(CachingConnectionFactory connectionFactory) {
        RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
        /*
         * 设置事务
         */
        rabbitTemplate.setChannelTransacted(rabbitInfo.isTransactional());
        /*
         * 不使用默认的序列化/反序列化转为byte[]传输方式
         * 而是将对象转为json在转为byte[]方式
         */
        rabbitTemplate.setMessageConverter(JsonMessageConverter);
        /*
         *使用单独的发送连接，避免生产者由于各种原因阻塞而导致消费者同样阻塞
         */
        rabbitTemplate.setUsePublisherConnection(true);
        /*
         *设置当连接失败时(仅连接出现问题而不是执行错误)重试连接
         */
        RetryTemplate retryTemplate = new RetryTemplate();
        retryTemplate.setRetryPolicy(new SimpleRetryPolicy(rabbitInfo.getMaxAttempts()) {
            @Override
            public void close(RetryContext status) {
                status.setExhaustedOnly();
            }
        });
        rabbitTemplate.setRetryTemplate(retryTemplate);
        /*
         * rabbitTemplate并不同时支持事务和确认机制同时存在因此开启事务之后就不应当设置确认机制
         */
        if (rabbitInfo.isTransactional()) return rabbitTemplate;
        /*
         * 如果不是事务模式则开启确认模式
         */
        connectionFactory.setPublisherConfirms(true);
        connectionFactory.setPublisherReturns(true);
        /*
         * 当mandatory标志位设置为true时
         * 如果exchange根据自身类型和消息routingKey无法找到一个合适的queue存储消息
         * 那么broker会调用basic.return方法将消息返还给生产者
         * 当mandatory设置为false时，出现上述情况broker会直接将消息丢弃
         */
        rabbitTemplate.setMandatory(true);
        /*
         * 消息发送到RabbitMQ交换器,但无相应队列与交换器绑定时的回调
         */
        CallbackDefault callbackDefault = new CallbackDefault();
        rabbitTemplate.setReturnCallback(returnCallbackReplace == null ? callbackDefault : returnCallbackReplace);
        /*
         * 消息发送到RabbitMQ交换器后接收ack回调
         */
        rabbitTemplate.setConfirmCallback(confirmCallbackReplace == null ? callbackDefault : confirmCallbackReplace);
        return rabbitTemplate;
    }

    @Bean
    public Channel channel(CachingConnectionFactory connectionFactory) throws IOException {
        Channel channel = connectionFactory.createConnection().createChannel(rabbitInfo.isTransactional());
        /*
         * 如果开启了事务则使用事务channel反之使用确认模式
         */
        if (rabbitInfo.isTransactional()) channel.txSelect();
        else channel.confirmSelect();
        return channel;
    }

    @Bean
    public RabbitUtil rabbitUtil() {
        return new RabbitUtil();
    }
}
