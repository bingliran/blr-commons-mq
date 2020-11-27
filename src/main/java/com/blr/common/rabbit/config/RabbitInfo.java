package com.blr.common.rabbit.config;

import com.blr.common.auto.util.EnvironmentUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.AcknowledgeMode;

/**
 * rabbit配置对象
 */
@Getter
@Slf4j
public class RabbitInfo {

    /**
     * 队列交换机标识符
     */
    public static final String DEAD_LETTER_EXCHANGE_KEY = "x-dead-letter-exchange";
    /**
     * 队列交换机绑定键标识符
     */
    public static final String DEAD_LETTER_ROUTING_KEY = "x-dead-letter-routing-key";
    /**
     * 队列消息超时时间
     */
    public static final String MESSAGE_TTL = "x-message-ttl";

    public static final String IS_JMC = "isJsonMessageConverter";

    public static final String TRANSPORT = "com.blr.common.rabbitmq.transport.";
    /**
     * 是否持久化(消费者)
     */
    private boolean durable;
    /**
     * 当所有消费者断开连接后不自动删除(消费者)
     */
    private boolean autoDelete;
    /**
     * 私有队列(消费者)
     */
    private boolean exclusive;

    /**
     * 所有操作都是事务的(消费)
     */
    private boolean transactional;
    /**
     * 调度模式
     * 0总是以空闲的优先
     * 1为公平调度:一个服务一次(生产)
     */
    private int prefetchCount;

    /**
     * 重试次数(生产)
     */
    private int maxAttempts;

    /**
     * 确认模式 (消费)
     * NONE 自动确认
     * 只要消息从队列中获取，无论消费者获取到消息后是否有成功接收的反馈，都认为是消息已经被成功消费。
     * MANUAL 手动确认
     * 消费者从队列中获取消息后，服务器会将消息标记为不可用状态，等待消费者的反馈，如果消费者一直没有反馈，那么消息将一直处于不可用状态。
     * AUTO
     * 如果消息成功被消费（成功的意思就是在消费的过程中没有抛出异常），则自动确认
     * 当消费者抛出不可恢复的异常时,则消息会被拒绝，且requeue=false（不重新入队列）
     * 不可恢复的异常:ConditionalRejectingErrorHandler.isCauseFatal
     * 当抛出ImmediateAcknowledgeAmqpException异常，则消费者会被确认
     * 其他的异常，则消息会被拒绝，且requeue=true
     */
    private AcknowledgeMode acknowledgeMode;

    /**
     * 通用前缀 (消费者)
     */
    private String autoPrefix;

    /**
     * 死信前缀名称(消费者)
     */
    private String deadPrefix;

    /**
     * 重试延时时间(单位秒)(消费者)
     */
    private int retryDelayTime;

    /**
     * 传输时采用的字符集默认utf-8(生产,消费)
     */
    private String encoding;

    /**
     * 注解@Payload中指定类的包位置(消费)
     */
    private String classNamePathMark;

    /**
     * 用户信息
     */
    private String username;
    private String password;
    private String host;
    private int port;

    private String defaultR = "spring.rabbitmq.";

    public RabbitInfo(EnvironmentUtil environmentUtil) {
        this.autoDelete = environmentUtil.getBoolean(defaultR + "autoDelete", false);
        this.durable = environmentUtil.getBoolean(defaultR + "durable", true);
        this.exclusive = environmentUtil.getBoolean(defaultR + "exclusive", false);
        this.transactional = environmentUtil.getBoolean(defaultR + "transactional", false);
        this.prefetchCount = environmentUtil.getInt(defaultR + "prefetchCount", 1);
        this.maxAttempts = environmentUtil.getInt(defaultR + "maxAttempts", 1);
        String mode = environmentUtil.getString(defaultR + "acknowledgeMode", "MANUAL");
        this.autoPrefix = environmentUtil.getString(defaultR + "autoPrefix", environmentUtil.getProjectName() + "_");
        this.deadPrefix = autoPrefix + environmentUtil.getString(defaultR + "deadPrefix", "dead_");
        this.retryDelayTime = environmentUtil.getInt(defaultR + "retryDelayTime", 0);
        this.username = environmentUtil.getString(defaultR + "username", null);
        this.password = environmentUtil.getString(defaultR + "password", null);
        this.host = environmentUtil.getString(defaultR + "host", null);
        this.port = environmentUtil.getInt(defaultR + "port", -1);
        this.classNamePathMark = environmentUtil.getString(defaultR + "classNamePathMark", null);
        this.encoding = environmentUtil.getString(defaultR + "encoding", "UTF-8");
        switch (mode.toUpperCase()) {
            case "NONE":
                this.acknowledgeMode = AcknowledgeMode.NONE;
                break;
            case "MANUAL":
                this.acknowledgeMode = AcknowledgeMode.MANUAL;
                break;
            default:
                log.error("RabbitMQ未知的确认模式:[{}],重置为:[{}]", mode, AcknowledgeMode.AUTO);
            case "AUTO":
                this.acknowledgeMode = AcknowledgeMode.AUTO;
                break;
        }
    }


}
