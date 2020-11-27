package com.blr.common.rocket.exception;

/**
 * 将rocket抛出的发送消息连接异常统一为此异常
 */
public class RocketSendMessageException extends RuntimeException {

    public RocketSendMessageException() {
        super();
    }

    public RocketSendMessageException(String message) {
        super(message);
    }

    public RocketSendMessageException(String message, Throwable cause) {
        super(message, cause);
    }

    public RocketSendMessageException(Throwable cause) {
        super(cause);
    }

    public RocketSendMessageException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
