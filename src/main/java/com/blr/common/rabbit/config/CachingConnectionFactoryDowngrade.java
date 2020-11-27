package com.blr.common.rabbit.config;

import com.rabbitmq.client.ShutdownSignalException;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.RabbitUtils;

/**
 * 异常降级处理
 */
public class CachingConnectionFactoryDowngrade extends CachingConnectionFactory {
    @Override
    public void shutdownCompleted(ShutdownSignalException cause) {
        if (!RabbitUtils.isNormalChannelClose(cause)) {
            logger.warn("ack或nack/reject被调用2次或以上:" + cause.getMessage());
            int protocolClassId = cause.getReason().protocolClassId();
            if (protocolClassId == RabbitUtils.CHANNEL_PROTOCOL_CLASS_ID_20) {
                getChannelListener().onShutDown(cause);
            } else if (protocolClassId == RabbitUtils.CONNECTION_PROTOCOL_CLASS_ID_10) {
                getConnectionListener().onShutDown(cause);
            }
        } else super.shutdownCompleted(cause);
    }
}
