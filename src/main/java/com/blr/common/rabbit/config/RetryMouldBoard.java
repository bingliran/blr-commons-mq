package com.blr.common.rabbit.config;

import com.blr.common.rabbit.util.RabbitUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;

import java.io.IOException;

/**
 * 重试模板
 */
@Slf4j
public abstract class RetryMouldBoard {

    abstract public void doRun() throws IOException;

    public RetryMouldBoard(RabbitUtil rabbitUtil) {
        retry(rabbitUtil);
    }

    public void retry(RabbitUtil rabbitUtil) {
        try {
            doRun();
        } catch (IOException e) {
            try {
                rabbitUtil.retrySleep();
                doRun();
            } catch (Exception ex) {
                log.error("retry发起重试失败", ex);
                throw new AmqpRejectAndDontRequeueException(ex);
            }
        }
    }
}
