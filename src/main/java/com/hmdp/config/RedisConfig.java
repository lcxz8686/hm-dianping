package com.hmdp.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 配置 Redisson
 */
@Configuration
public class RedisConfig {
    @Bean
    public RedissonClient redissonClient() {

        // 配置
        Config config = new Config();
        config.useSingleServer().setAddress("redis://192.168.89.128:6379").setPassword("888888");

        // 创建 RedissonClient 对象
        return Redisson.create(config);
    }
}
