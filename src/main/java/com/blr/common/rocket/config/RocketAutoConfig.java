package com.blr.common.rocket.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import javax.annotation.PostConstruct;

/**
 * 自动配置
 */
@Configuration
@Import(RocketConfig.class)
@Slf4j
public class RocketAutoConfig {

    @PostConstruct
    public void postConstruct() {
        log.info("rocket装载完成");
    }
}
