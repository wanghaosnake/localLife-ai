package com.locallife;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

@MapperScan("com.locallife.mapper")
@SpringBootApplication
@EnableAspectJAutoProxy(exposeProxy=true)
public class LocalLifePingApplication {

    public static void main(String[] args) {
        SpringApplication.run(LocalLifePingApplication.class, args);
    }

}
