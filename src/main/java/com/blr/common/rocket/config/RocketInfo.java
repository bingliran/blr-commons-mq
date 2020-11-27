package com.blr.common.rocket.config;

import com.blr.common.auto.util.EnvironmentUtil;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;

import java.util.HashMap;
import java.util.Map;

/**
 * rocket配置信息
 */
public class RocketInfo {
    public static final String GENERATE_PRODUCER_GROUP = "GENERATE_" + MixAll.CLIENT_INNER_PRODUCER_GROUP;
    private final String path = "spring.rocketmq.";
    private final String defaultR = path + "default.";
    private final Map<String, Entity> entityMap = new HashMap<>();

    /**
     * 使用内部类为后续手动定义特供便捷
     */
    private final Entity entity = new Entity();

    /**
     * 是否开启RocketAdminUtil
     */
    private boolean enableRocketAdminUtil;

    /**
     * 是否使用AlwaysRocketUtil
     */
    private boolean alwaysGenerateProducer;

    public RocketInfo(EnvironmentUtil environmentUtil) {
        initEntity(environmentUtil, defaultR, entity);
        alwaysGenerateProducer = environmentUtil.getBoolean(path + "alwaysGenerateProducer", false);
        if (alwaysGenerateProducer) System.setProperty(GENERATE_PRODUCER_GROUP, String.valueOf(true));
        enableRocketAdminUtil = environmentUtil.getBoolean(path + "adminUtil", false);
        if (enableRocketAdminUtil) System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, entity.host);
        String names = environmentUtil.getString(path + "names", null);
        if (StringUtils.isBlank(names)) return;
        for (String name : names.split(";")) {
            Entity entity = initEntity(environmentUtil, path + name + ".", new Entity());
            entityMap.put(name, entity);
        }
    }

    private Entity initEntity(EnvironmentUtil environmentUtil, String defaultPath, Entity entity) {
        entity.topic = environmentUtil.getString(defaultPath + "topic", null);
        entity.host = environmentUtil.getString(defaultPath + "host", null);
        entity.isVip = environmentUtil.getBoolean(defaultPath + "isVip", true);
        entity.group = environmentUtil.getString(defaultPath + "group", environmentUtil.getProjectName());
        entity.tag = environmentUtil.getString(defaultPath + "tag", null);
        entity.timeOut = environmentUtil.getInt(defaultPath + "timeOut", 3000);
        entity.id = environmentUtil.getString(defaultPath + "id", null);
        return entity;
    }

    public Entity getEntityToName(String name) {
        return entityMap.get(name);
    }

    public String getTopic() {
        return entity.topic;
    }

    public String getHost() {
        return entity.host;
    }

    public boolean isVip() {
        return entity.isVip;
    }

    public String getGroup() {
        return entity.group;
    }

    public String getTag() {
        return entity.tag;
    }

    public int getTimeout() {
        return entity.timeOut;
    }

    public boolean isEnableRocketAdminUtil() {
        return enableRocketAdminUtil;
    }

    public boolean isAlwaysGenerateProducer() {
        return alwaysGenerateProducer;
    }

    public Entity getEntity() {
        return entity;
    }


    @Getter
    @Setter
    public static class Entity {
        /**
         * 主题名称(可以理解为需要发送到的队列的主题)
         */
        private String topic;
        /**
         * 地址 ip:port可以写多个用;隔开
         */
        private String host;
        /**
         * 是否为vip通道消息
         */
        private boolean isVip;
        /**
         * 连接在哪个分组里
         */
        private String group;
        /**
         * 标记名称(通过这个标记寻找有效的信息)
         */
        private String tag;

        /**
         * 请求超时时间
         */
        private int timeOut;

        /**
         * 配合abstractProducer
         */
        private String id;

        public Entity() {
        }
    }
}
