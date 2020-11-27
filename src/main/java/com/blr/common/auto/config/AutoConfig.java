package com.blr.common.auto.config;

import com.blr.common.auto.aspect.LogAspect;
import com.blr.common.auto.util.EnvironmentUtil;
import com.blr.common.auto.util.LogUtil;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

/**
 * 通用配置类
 */
@Configuration
@Lazy
public class AutoConfig {
    @Bean
    public EnvironmentUtil environmentUtil() {
        return new EnvironmentUtil();
    }

    @Bean
    public LogAspect logAspect() {
        return new LogAspect();
    }

    @Bean
    public LogUtil logUtil() {
        return new LogUtil();
    }
}
