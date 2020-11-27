package com.blr.common.rocket.config;

import com.blr.common.auto.util.EnvironmentUtil;
import com.blr.common.rocket.util.AbstractProducer;
import com.blr.common.rocket.util.AlwaysRocketUtil;
import com.blr.common.rocket.util.RocketAdminUtil;
import com.blr.common.rocket.util.RocketUtil;
import org.apache.rocketmq.client.exception.MQClientException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

import javax.annotation.PostConstruct;

/**
 * 提供rocket的一些配置
 */
public class RocketConfig {
    @Autowired
    private EnvironmentUtil environmentUtil;
    @Autowired
    private RocketInfo rocketInfo;

    @Bean
    public RocketUtil rocketUtil() {
        return rocketInfo.isAlwaysGenerateProducer() ? new AlwaysRocketUtil() : new RocketUtil();
    }

    @Bean
    public RocketInfo rocketInfo() {
        return new RocketInfo(environmentUtil);
    }

    @PostConstruct
    public void initRocketAdminUtil() throws MQClientException {
        RocketAdminUtil.getInstance(rocketInfo.isEnableRocketAdminUtil(), environmentUtil);
    }

    @Bean(name = "defaultProducer")
    @ConditionalOnMissingBean(name = "defaultProducer")
    public AbstractProducer defaultProducer(RocketUtil rocketUtil, RocketInfo rocketInfo) throws MQClientException {
        AbstractProducer producer = rocketUtil.generateProducer(rocketInfo);
        if (rocketInfo.isAlwaysGenerateProducer()) return producer;
        producer.setTransactionListener(new DefaultTransactionListener());
        producer.start();
        return producer;
    }

}
