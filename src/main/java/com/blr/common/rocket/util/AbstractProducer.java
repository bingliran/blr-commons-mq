package com.blr.common.rocket.util;

import com.blr.common.rocket.config.RocketInfo;
import com.blr.common.rocket.config.RocketInfo;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;

import java.io.Closeable;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 一个简化创建并且区分实例的工厂方法
 */
@Slf4j
public abstract class AbstractProducer extends TransactionMQProducer implements Closeable {
    private static Map<String, AbstractProducer> producerMap = new ConcurrentHashMap<>();

    static AbstractProducer generateProducer(RocketInfo.Entity entity, TransactionListener transactionListener) {
        return new CustomizeProducer(entity, transactionListener);
    }

    static void shutdown(String id) {
        AbstractProducer producer = producerMap.remove(id);
        if (producer != null) producer.shutdown();
    }

    static void shutdownAll() {
        producerMap.values().forEach(AbstractProducer::shutdown);
        producerMap.clear();
    }

    public abstract RocketInfo.Entity getEntity();

    @Override
    public abstract void close();

    @Getter
    /**
     * 套娃
     */
    private static class CustomizeProducer extends AbstractProducer {
        private RocketInfo.Entity entity;

        CustomizeProducer(RocketInfo.Entity entity, TransactionListener transactionListener) {
            String id = null;
            try {
                this.setProducerGroup(entity.getGroup());
                this.setNamesrvAddr(entity.getHost());
                this.setVipChannelEnabled(entity.isVip());//不愧是国产代码
                this.setSendMsgTimeout(entity.getTimeOut());
                this.entity = entity;
                String g = System.getProperty(RocketInfo.GENERATE_PRODUCER_GROUP);
                id = StringUtils.isNotBlank(g) || StringUtils.isBlank(entity.getId()) ? String.valueOf(UUID.randomUUID()) : entity.getId();
                if (producerMap.get(id) != null) {
                    AbstractProducer.shutdown(id);
                    log.warn("id:" + id + "出现重复,已删除之前的producer并且已经shutdown");
                }
                this.setInstanceName(id);
                this.entity.setId(id);
                if (transactionListener != null) this.setTransactionListener(transactionListener);
                producerMap.put(id, this);
            } catch (Exception e) {
                if (StringUtils.isNotBlank(id)) producerMap.remove(id);
                throw e;
            }
        }

        @Override
        public void close() {
            AbstractProducer.shutdown(this.getInstanceName());
        }

        @Override
        public void shutdown() {
            if (this.defaultMQProducerImpl != null) this.defaultMQProducerImpl.shutdown();
            try {
                this.defaultMQProducerImpl.destroyTransactionEnv();
            } catch (NullPointerException e) {
                if (log.isTraceEnabled())
                    log.trace("在未初始化时调用了shutdown,group:{},id:{}", this.getProducerGroup(), this.getInstanceName());
            }
        }
    }
}
