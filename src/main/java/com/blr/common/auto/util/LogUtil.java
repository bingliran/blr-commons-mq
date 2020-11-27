package com.blr.common.auto.util;

import com.blr.common.auto.transport.LogInfo;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * 既保证实时性又保证区块顺序
 * 如果只是一句日志,并不建议使用LogUtil
 */
@Slf4j
@SuppressWarnings("unused")
public class LogUtil extends Thread {
    /**
     * 其中finish为结束标记不可使用
     * trace未在log范围中
     */
    public static final int info = 1;
    public static final int debug = 2;
    public static final int warn = 3;
    public static final int error = 4;

    private static final int finish = 5;

    private BlockingQueue<String> readQueue = new LinkedBlockingQueue<>();
    private Map<String, LinkedBlockingQueue<LogInfo>> readMap = new ConcurrentHashMap<>();

    @PostConstruct
    public void postConstruct() {
        this.setDaemon(true);
        this.start();
        if (log.isDebugEnabled()) log.debug("LogUtil初始化完成");
    }

    public LogUtil info(String log, Object... arg) {
        return addLog(info, null, log, null, arg);
    }

    public LogUtil info(String log, Throwable e) {
        return addLog(info, null, log, e);
    }

    public LogUtil debug(String log, Object... arg) {
        return addLog(debug, null, log, null, arg);
    }

    public LogUtil debug(String log, Throwable e) {
        return addLog(debug, null, log, e);
    }

    public LogUtil warn(String log, Object... arg) {
        return addLog(warn, null, log, null, arg);
    }

    public LogUtil warn(String log, Throwable e) {
        return addLog(warn, null, log, e);
    }

    public LogUtil error(String log, Object... arg) {
        return addLog(error, null, log, null, arg);
    }

    public LogUtil error(String log, Throwable e) {
        return addLog(error, null, log, e);
    }

    public LogUtil info(Logger logger, String log, Object... arg) {
        return addLog(info, logger, log, null, arg);
    }

    public LogUtil info(Logger logger, String log, Throwable e) {
        return addLog(info, logger, log, e);
    }

    public LogUtil debug(Logger logger, String log, Object... arg) {
        return addLog(debug, logger, log, null, arg);
    }

    public LogUtil debug(Logger logger, String log, Throwable e) {
        return addLog(debug, logger, log, e);
    }

    public LogUtil warn(Logger logger, String log, Object... arg) {
        return addLog(warn, logger, log, null, arg);
    }

    public LogUtil warn(Logger logger, String log, Throwable e) {
        return addLog(warn, logger, log, e);
    }

    public LogUtil error(Logger logger, String log, Object... arg) {
        return addLog(error, logger, log, null, arg);
    }

    public LogUtil error(Logger logger, String log, Throwable e) {
        return addLog(error, logger, log, e);
    }

    private LogUtil addLog(int level, Logger logger, String log, Throwable e, Object... arg) {
        String id = String.valueOf(Thread.currentThread().getId());
        StackTraceElement path = Thread.currentThread().getStackTrace()[3];
        if (logger == null) logger = LoggerFactory.getLogger(path.getClassName());
        LogInfo logInfo = new LogInfo(id, log, arg, level, logger == null ? LogUtil.log : logger, path, e);
        if (readMap.get(id) == null) readMap.put(id, new LinkedBlockingQueue<>());
        readMap.get(id).offer(logInfo);
        readQueue.offer(id);
        return this;
    }

    /**
     * 当本次顺序输入结束后应当调用finish方法以正确结束
     * <p>
     * 请谨慎使用在同一线程中调用多次finish
     */
    public void finish() {
        String id = String.valueOf(Thread.currentThread().getId());
        BlockingQueue<LogInfo> queue = readMap.get(id);
        if (queue == null) return;
        LogInfo logInfo = new LogInfo();
        logInfo.setId(id);
        logInfo.setLevel(finish);
        queue.offer(logInfo);
    }

    /**
     * 使用LinkedBlockingQueue,ConcurrentHashMap互相套用减少LinkedBlockingQueue的队列复杂性
     * 获取log的超时时间为10秒:为了方式未调用finish导致线程卡住
     */
    @Override
    public void run() {
        while (!isInterrupted()) {
            String id = null;
            LogInfo logInfo = null;
            try {
                id = readQueue.take();
                BlockingQueue<LogInfo> queue = readMap.get(id);
                if (queue == null) continue;
                while (!isInterrupted()) {
                    LogInfo thisLogInfo;
                    logInfo = (thisLogInfo = queue.poll(10, TimeUnit.SECONDS)) == null ? logInfo : thisLogInfo;
                    if (logInfo == null) break;
                    if (thisLogInfo == null)
                        throw new TimeoutException("等待" + logInfo.getPath() + "的下一条log超时,也或者忘记调用finish方法?");
                    if (thisLogInfo.getLevel() == finish) break;
                    caseLog(thisLogInfo);
                }
            } catch (TimeoutException e) {
                if (LogUtil.log.isDebugEnabled()) LogUtil.log.debug(e.getMessage());
            } catch (Exception e) {
                LogUtil.log.error("顺序消息出错", e);
            } finally {
                /*
                 * 结束时删除id下的所有防止冗余
                 */
                if (id != null) {
                    BlockingQueue<LogInfo> queue = readMap.remove(id);
                    if (queue != null) {
                        /*
                         * 清理finish之后或者timeOut之后由于使用不当造成的冗余
                         */
                        List<LogInfo> list = new ArrayList<>();
                        int wrongNumber = queue.drainTo(list);
                        if (wrongNumber > 0) {
                            (list = list.stream().filter((wrongLog) -> wrongLog.getLevel() != finish).collect(Collectors.toList())).forEach(this::caseLog);
                            LogUtil.log.warn("在finish之后又追加了至少{}条数据,且在此过程中调用了{}次finish", list.size(), wrongNumber - list.size());
                        }
                    }
                }
            }
        }
    }

    private void caseLog(LogInfo logInfo) {
        if (logInfo.getThrowable() == null) eventLog(logInfo);
        else exceptionLog(logInfo);
    }

    private void eventLog(LogInfo logInfo) {
        Logger logger = logInfo.getLogger();
        switch (logInfo.getLevel()) {
            case info:
                logger.info(logInfo.getLog(), logInfo.getArg());
                break;
            case debug:
                if (logger.isDebugEnabled())
                    logger.debug(logInfo.getLog(), logInfo.getArg());
                break;
            case warn:
                logger.warn(logInfo.getLog(), logInfo.getArg());
                break;
            case error:
                logger.error(logInfo.getLog(), logInfo.getArg());
                break;
        }
    }

    private void exceptionLog(LogInfo logInfo) {
        Logger logger = logInfo.getLogger();
        switch (logInfo.getLevel()) {
            case info:
                logger.info(logInfo.getLog(), logInfo.getThrowable());
                break;
            case debug:
                if (logger.isDebugEnabled())
                    logger.debug(logInfo.getLog(), logInfo.getThrowable());
                break;
            case warn:
                logger.warn(logInfo.getLog(), logInfo.getThrowable());
                break;
            case error:
                logger.error(logInfo.getLog(), logInfo.getThrowable());
                break;
        }
    }

    @Override
    public void interrupt() {
        super.interrupt();
        readQueue.clear();
        readMap.clear();
        readQueue = null;
        readMap = null;
        throw new IllegalArgumentException("logUtil被终止");
    }
}
