package com.blr.common.auto.transport;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * 通用的请求参数
 */
@Getter
@Setter
@ToString
public class RequestData<T> {

    /**
     * 请求说明
     * 比如:我这是扣款结果通知
     */
    private String type;

    /**
     * 可以是map或者实体对象等
     * 注:实体对象必须存在get方法
     */
    private T data;

    /**
     * 请求标识
     */
    private String topic;

    /**
     * 消息tag
     */
    private String tag;

    /**
     * 消息key
     */
    private String key;
}
