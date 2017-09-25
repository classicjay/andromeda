package com.bonc.dw3.common;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;
import org.springframework.web.client.RestTemplate;

import java.lang.reflect.Method;

//@Aspect
//@Component
public class MapperAroundAspect implements EnvironmentAware {

    /**
     * 日志对象
     */
    private Logger logger = LoggerFactory.getLogger(MapperAroundAspect.class);

    @Autowired
    private Environment env;

    @Autowired
    RestTemplate restTemplate;


    // service层的统计耗时切面，类型必须为final String类型的,注解里要使用的变量只能是静态常量类型的
    public static final String POINT = "execution (* com.bonc.dw3.mapper.*.*(..))";

    /**
     * 统计方法执行耗时Around环绕通知
     *
     * @param joinPoint
     * @return
     */
    @Around(POINT)
    public Object around(ProceedingJoinPoint joinPoint) throws Throwable {
        Object obj = null;
        //查看mapper方法上是否有注解MapperResponseTypeAnnotation
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Method targetMethod = signature.getMethod();
        logger.info("拦截到"+signature.getName()+"方法");
        Class clazz = targetMethod.getClass();
        Method realMethod = joinPoint.getTarget().getClass().getDeclaredMethod(signature.getName(), targetMethod.getParameterTypes());
        //存在调用自己的逻辑
        if(targetMethod.isAnnotationPresent(MapperResponseTypeAnnotation.class)){
            logger.info("拦截到"+signature.getName()+"方法，剩下的逻辑自己处理");
            
        }else{//否则按原样放回
            Object[] args = joinPoint.getArgs();
            try {
                obj = joinPoint.proceed(args);
            } catch (Throwable e) {
                logger.error("MapperBeforeAspect调用异常", e);
                throw e;
            }
        }
        return obj;
    }


    @Override
    public void setEnvironment(Environment environment) {
        this.env = environment;
    }
}
