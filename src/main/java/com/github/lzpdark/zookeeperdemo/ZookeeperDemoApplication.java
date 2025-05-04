package com.github.lzpdark.zookeeperdemo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class ZookeeperDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(ZookeeperDemoApplication.class, args);
    }

}
