package com.blr.common.rocket.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * 默认的事务消息监听
 */
@Slf4j
public class DefaultTransactionListener implements TransactionListener {
    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        if (log.isDebugEnabled()) log.debug("TransactionMessage:{}, 附加参数:{}", msg, arg);

        return LocalTransactionState.COMMIT_MESSAGE;
    }

    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        return LocalTransactionState.COMMIT_MESSAGE;
    }
}
