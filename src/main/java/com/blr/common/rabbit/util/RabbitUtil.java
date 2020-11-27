package com.blr.common.rabbit.util;

import com.alibaba.fastjson.JSONObject;
import com.blr.common.auto.transport.RequestData;
import com.blr.common.rabbit.config.RabbitInfo;
import com.blr.common.rabbit.config.RetryMouldBoard;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * 提供rabbitMQ的一些便捷方式
 */
@Slf4j
public class RabbitUtil {

    @Autowired
    private RabbitInfo rabbitInfo;

    /**
     * 创建一个durable=true(持久化)
     * exclusive=false(非私有队列)
     * autoDelete=false(当所有消费者断开连接后不自动删除)
     * 拥有queueName如果当前名称存在则不创建的队列
     */
    public Queue createQueue(String queueName) {
        return new Queue(queueName, rabbitInfo.isDurable(), rabbitInfo.isExclusive(), rabbitInfo.isAutoDelete());
    }

    /**
     * 创建一个通配符交换机
     * 通配符交换机:
     * 主要有两种通配符：# 和 *
     * *（星号）：可以（只能）匹配一个单词
     * #（井号）：可以匹配多个单词（或者零个）
     * 也就是Queue可以用 *.name.#这样的方式去匹配Exchange
     */
    public TopicExchange createTopicExchange(String queueExchangeName) {
        return new TopicExchange(queueExchangeName, rabbitInfo.isDurable(), rabbitInfo.isAutoDelete());
    }

    /**
     * DirectExchange是默认的Exchange，
     * 在使用这个类型的Exchange时,可以不必指定routing key的名字,
     * 在此类型下创建的Queue有一个默认的routing key,这个routing key与Queue同名。
     */
    public DirectExchange createDirectExchange(String queueExchangeName) {
        return new DirectExchange(queueExchangeName, rabbitInfo.isDurable(), rabbitInfo.isAutoDelete());
    }


    /**
     * 使用这种类型的Exchange,会忽略routing key的存在，
     * 直接将message广播到所有的Queue中。
     */
    public FanoutExchange createFanoutExchange(String queueExchangeName) {
        return new FanoutExchange(queueExchangeName, rabbitInfo.isDurable(), rabbitInfo.isAutoDelete());
    }

    /**
     * 它是根据Message的一些头部信息来分发过滤Message,
     * 忽略routing key的属性,如果Header信息和message消息的头信息相匹配,
     * 那么这条消息就匹配上了
     */
    public HeadersExchange createHeadersExchange(String queueExchangeName) {
        return new HeadersExchange(queueExchangeName, rabbitInfo.isDurable(), rabbitInfo.isAutoDelete());
    }

    /**
     * 除了FanoutExchange,HeadersExchange之外的Exchange都需要进行绑定
     * 为null时默认采用queue的名字
     */
    public Binding createBinding(Queue queue, AbstractExchange exchange, String routeKey) {
        if (routeKey == null) routeKey = queue.getName();
        return BindingBuilder.bind(queue).to(exchange).with(routeKey).noargs();
    }

    public Binding createBinding(Queue queue, AbstractExchange exchange) {
        return createBinding(queue, exchange, null);
    }

    /**
     * 下面的方法通过channel声明 queue,exchange,binding而不是通过new
     * 这种方式不会将queue,exchange,binding注册为bean也不会产生对象信息
     */
    @Autowired
    private Channel channel;

    /**
     * 通过Channel声明一个完整的流程包括死信队列
     * 注:死信交换机类型也是当前队列交换机类型
     *
     * @param queueName    队列名称
     * @param exchangeName 交换机名称
     * @param routeKey     绑定名称
     * @param type         交换机类型
     */
    public void createConnection(String queueName, String exchangeName, String routeKey, BuiltinExchangeType type) throws IOException {
        routeKey = routeKey == null ? queueName : routeKey;
        /*
         * 先声明死信queue,exchange,binding
         */
        exchangeDeclare(rabbitInfo.getDeadPrefix() + exchangeName, BuiltinExchangeType.DIRECT, null);
        deadDeclareQueueAndBind(queueName, exchangeName, routeKey);
        /*
         * 然后声明需要使用的queue,exchange,binding并将queue的死信队列绑定到当前死信队列
         */
        exchangeDeclare(exchangeName, type, null);
        declareQueueAndBind(queueName, exchangeName, routeKey, getDeadArgs(exchangeName, routeKey));
        if (rabbitInfo.isTransactional()) channel.txCommit();
    }

    /**
     * 在交换机中新增一个队列并增加死信队列
     * 注:如果交换机不存在则不会声明
     * 如果交换机存在并且队列也存在那么队列会直接添加到交换机当中而不是声明新的队列
     */
    public void addQueue(String queueName, String exchangeName, String routeKey) throws IOException {
        routeKey = routeKey == null ? queueName : routeKey;
        deadDeclareQueueAndBind(queueName, exchangeName, routeKey);
        declareQueueAndBind(queueName, exchangeName, routeKey, getDeadArgs(exchangeName, routeKey));
        if (rabbitInfo.isTransactional()) channel.txCommit();
    }

    /**
     * 声明一个死信绑定参数
     */
    public Map<String, Object> getDeadArgs(String exchangeName, String routeKey) {
        Map<String, Object> args = new HashMap<>(2);
        args.put(RabbitInfo.DEAD_LETTER_EXCHANGE_KEY, rabbitInfo.getDeadPrefix() + exchangeName);
        args.put(RabbitInfo.DEAD_LETTER_ROUTING_KEY, rabbitInfo.getDeadPrefix() + routeKey);
        //args.put("x-message-ttl", 2000);
        return args;
    }

    /**
     * 声明一个queue
     * 注:args并不是必要参数
     */
    public void queueDeclare(String queueName, Map<String, Object> args) throws IOException {
        channel.queueDeclare(queueName, rabbitInfo.isDurable(), rabbitInfo.isExclusive(), rabbitInfo.isAutoDelete(), args);
        //channel.basicConsume(queueName, retryConsumer);
    }

    /**
     * 声明一个exchange
     * 注:args并不是必要参数
     */
    public void exchangeDeclare(String exchangeName, BuiltinExchangeType type, Map<String, Object> args) throws IOException {
        channel.exchangeDeclare(exchangeName, type, rabbitInfo.isDurable(), rabbitInfo.isAutoDelete(), args);
    }

    /**
     * 绑定一个queue到exchange
     */
    public void queueBind(String queueName, String exchangeName, String routeKey) throws IOException {
        channel.queueBind(queueName, exchangeName, routeKey);
    }

    /**
     * 声明一个死信队列并绑定到指定交换机上
     * 注:声明的死信队列如果没有队列绑定此队列那它就是一个以死信前缀开头的正常队列
     */
    public void deadDeclareQueueAndBind(String queueName, String exchangeName, String routeKey) throws IOException {
        routeKey = routeKey == null ? queueName : routeKey;
        queueDeclare(rabbitInfo.getDeadPrefix() + queueName, null);
        queueBind(rabbitInfo.getDeadPrefix() + queueName, rabbitInfo.getDeadPrefix() + exchangeName, rabbitInfo.getDeadPrefix() + routeKey);
    }

    /**
     * 声明一个队列并绑定到指定交换机上
     */
    public void declareQueueAndBind(String queueName, String exchangeName, String routeKey, Map<String, Object> args) throws IOException {
        routeKey = routeKey == null ? queueName : routeKey;
        queueDeclare(queueName, args);
        queueBind(queueName, exchangeName, routeKey);
    }

    @Autowired
    private RabbitTemplate rabbitTemplate;

    /**
     * 重试
     * 手动模式下如果需要重试请确保没调用过basicNack,basicAck
     * 其他模式下请确认最后执行结束后的结果是失败
     */
    public void retry(final Channel channel, final Message message) {
        Object retryHeader = message.getMessageProperties().getHeaders().get("retry");
        if (retryHeader == null) {
            log.warn("retry获取剩余重试次数失败未进行重试");
            return;
        }
        int retry = Integer.parseInt(String.valueOf(retryHeader));
        if (retry <= 0) {
            /*
             * 当重试次数用尽后发送失败结果
             * 这里必须发送失败即使在非手动模式下会出现channel被关闭的警告
             * 不然死信队列将无法收到失效消息
             */
            new RetryMouldBoard(this) {
                @Override
                public void doRun() throws IOException {
                    basicNack(channel, message);
                }
            };
        } else {
            message.getMessageProperties().getHeaders().put("retry", --retry);
            if (isMANUAL()) {
                new RetryMouldBoard(this) {
                    @Override
                    public void doRun() throws IOException {
                        basicAck(channel, message);
                    }
                };
            }
            /*
             * 添加消息
             */
            new RetryMouldBoard(this) {
                @Override
                public void doRun() throws IOException {
                    basicPublish(channel, message);
                }
            };
        }
    }

    /**
     * 将当前消息添加到当前队列的队首
     */
    public void basicPublish(Channel channel, Message message) throws IOException {
        if (channel == null || !channel.isOpen()) channel = this.channel;
        channel.basicPublish(message.getMessageProperties().getReceivedExchange(),
                message.getMessageProperties().getReceivedRoutingKey(),
                false,
                new AMQP.BasicProperties().builder().contentType(message.getMessageProperties().getContentType()).headers(message.getMessageProperties().getHeaders()).build(),
                message.getBody());
    }

    /**
     * 重试的等待时间
     */
    public void retrySleep() {
        try {
            Thread.sleep(rabbitInfo.getRetryDelayTime() * 1000);
        } catch (Exception e) {
            log.warn("retrySleep被唤醒", e);
        }
    }

    /**
     * 返回当前Channel失败结果
     */
    public void basicNack(Channel channel, Message message) throws IOException {
        channel.basicNack(message.getMessageProperties().getDeliveryTag(), false, false);
    }

    public void basicAck(Channel channel, Message message) throws IOException {
        channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
    }

    /**
     * 判断是否为手动模式
     */
    public boolean isMANUAL() {
        return !rabbitInfo.isTransactional() && rabbitInfo.getAcknowledgeMode().equals(AcknowledgeMode.MANUAL);
    }


    @PreDestroy
    public void closeChannel() throws IOException, TimeoutException {
        this.channel.close();
    }

    /**
     * 发送
     * 使用这种方式发送消费者将不能以jsonString接受
     */
    public void send(String exchange, String routeKey, Object data) {
        MessageProperties messageProperties = new MessageProperties();
        messageProperties.setHeader(RabbitInfo.IS_JMC, true);
        Message message = rabbitTemplate.getMessageConverter().toMessage(data, messageProperties);
        message.getMessageProperties().getHeaders().put("retry", rabbitInfo.getMaxAttempts());
        rabbitTemplate.send(exchange, routeKey, message);
    }

    /**
     * 转为json发送,
     * 使用这种方式发送时消费者只能以string形式接受
     */
    public void sendAndConvert(String exchange, String routeKey, RequestData<?> data) {
        data.setTopic(routeKey);
        send(exchange, routeKey, JSONObject.toJSONString(data));
    }

}

