package com.bonc.dw3.common;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Created by zg on 2017/8/10.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface MapperResponseTypeAnnotation {
    String value();

    public static final String MAPPER_RESPONSE_TYPE_TXT = "TXT";
}
