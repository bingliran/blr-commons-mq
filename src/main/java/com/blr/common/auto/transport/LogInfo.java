package com.blr.common.auto.transport;

import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;

@Getter
@Setter
public class LogInfo {
    private String id;
    private String log;
    private Object[] arg;
    private int level;
    private Logger logger;
    private StackTraceElement path;
    private Throwable throwable;

    public LogInfo(String id, String log, Object[] arg, int level, Logger logger, StackTraceElement path, Throwable e) {
        this.id = id;
        this.log = log;
        this.arg = arg;
        this.level = level;
        this.logger = logger;
        this.path = path;
        this.throwable = e;
    }

    public LogInfo() {
    }
}