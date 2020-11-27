package com.blr.common.rocket.config;

import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;

/**
 * 顺序消息
 */
public class OrderMessage implements MessageQueueSelector {
    @Override
    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
        /*
         * &mqs.size() - 1 使结果不会大于 mqs.size-1
         * 指定消息进入第i个队列
         * arg%mqs.size 这种方法就约定了arg只能 instanceof number.class
         */
        int i = (mqs.size() - 1) & arg.hashCode();
        return mqs.get(i);
    }
}
