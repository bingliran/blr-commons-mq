package com.blr.common.rabbit.config;

import org.springframework.amqp.rabbit.listener.ConditionalRejectingErrorHandler;
import org.springframework.messaging.converter.MessageConversionException;

/**
 * 不可恢复异常判定增加
 */
public class UserCauseExceptionStrategy extends ConditionalRejectingErrorHandler {

    public UserCauseExceptionStrategy() {
        super(new ConditionalRejectingErrorHandler.DefaultExceptionStrategy() {
            @Override
            protected boolean isUserCauseFatal(Throwable cause) {
                /*
                 * 非运行时异常和转换异常判定为不可恢复异常
                 */
                return !(cause instanceof RuntimeException) || cause instanceof MessageConversionException;
            }
        });
        /*
         * 如果拥有XDeath则使用XDeath
         */
        this.setDiscardFatalsWithXDeath(true);
    }

    @Override
    protected void log(Throwable t) {
        logger.error("出现未经检测的异常", t);
    }
}