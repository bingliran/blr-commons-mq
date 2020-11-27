package com.blr.common.auto.aspect;

import com.blr.common.auto.util.LogUtil;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Arrays;

/**
 * 日志
 */
@Aspect
public class LogAspect {
    @Autowired
    private LogUtil logUtil;

    @Around("execution (* com.blr.common.*.util.*Util.send*(..))")
    public Object around(ProceedingJoinPoint joinPoint) throws Throwable {
        logUtil.info("拉起:{}", joinPoint.getSignature());
        logUtil.info("请求参数:{}", Arrays.toString(joinPoint.getArgs()));
        long l = System.currentTimeMillis();
        Object result = null;
        Throwable throwable = null;
        try {
            if ((result = joinPoint.proceed()) != null) logUtil.info("返回结果:{}", result);
        } catch (Throwable e) {
            logUtil.error("出现异常:{}", e);
            throwable = e;
        }
        logUtil.info("请求结束,耗时{}毫秒", System.currentTimeMillis() - l).finish();
        if (throwable != null) throw throwable;
        return result;
    }
}

