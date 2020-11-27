package com.blr.common.rabbit.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import javax.annotation.PostConstruct;

@Configuration
@Import({RabbitConfig.class, RabbitAutoConfiguration.class})
@Slf4j
/**
 * 通过spring.factories配置自动加载并以移除懒加载避免首次使用延时
 */
public class RabbitAutoConfig {
    @PostConstruct
    public void postConstruct() {
        log.info("rabbit装载完成");
    }
}
