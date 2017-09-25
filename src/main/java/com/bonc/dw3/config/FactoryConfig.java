/*声明一个Bean，注入校验器到Spring Boot的运行环境，处理异常时用到*/

package com.bonc.dw3.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.beanvalidation.MethodValidationPostProcessor;

@Configuration
@EnableAutoConfiguration
public class FactoryConfig {
	
	final static Logger logger= LoggerFactory.getLogger(FactoryConfig.class);
	 
    @Bean
    public MethodValidationPostProcessor methodValidationPostProcessor(){
     return new MethodValidationPostProcessor();
    }
    

}
