package com.blr.common.auto.util;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;

/**
 * environment工具
 */
public class EnvironmentUtil {
    @Autowired
    private ConfigurableEnvironment environment;
    @Autowired
    private ConfigurableApplicationContext applicationContext;

    public boolean getBoolean(String key, boolean defaultValue) {
        if (environment == null) return defaultValue;
        return environment.getProperty(key, boolean.class, defaultValue);
    }

    public int getInt(String key, int defaultValue) {
        if (environment == null) return defaultValue;
        return environment.getProperty(key, int.class, defaultValue);
    }

    public String getProjectName() {
        return getString("spring.application.name", null);
    }

    public String getString(String key, String defaultValue) {
        if (environment == null) return defaultValue;
        return environment.getProperty(key, String.class, defaultValue);
    }

    /**
     * 手动注册一个bean已有同名bean时注册失败
     */
    public <T> T registerBean(String name, Class<T> clazz, Object... args) {
        if (applicationContext.containsBean(name)) return null;
        BeanDefinitionBuilder beanDefinitionBuilder = BeanDefinitionBuilder.genericBeanDefinition(clazz);
        for (Object arg : args) beanDefinitionBuilder.addConstructorArgValue(arg);
        BeanDefinitionRegistry beanFactory = (BeanDefinitionRegistry) applicationContext.getBeanFactory();
        beanFactory.registerBeanDefinition(name, beanDefinitionBuilder.getRawBeanDefinition());
        return applicationContext.getBean(name, clazz);
    }
}
