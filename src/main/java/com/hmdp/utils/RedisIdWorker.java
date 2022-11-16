package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Redis的全局ID生成器
 * 优惠秒杀业务
 */
@Component
public class RedisIdWorker {

    /**
     * 开始时间戳
     * 2022.1.1的时间戳
     */
    private static final long BEGIN_TIMESTAMP = 1640995200L;

    /**
     * 序列号的位数
     */
    private static final int COUNT_BITS = 32;

    private StringRedisTemplate stringRedisTemplate;

    // 构造器注入
    public RedisIdWorker(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    /**
     * 生成全局ID
     * Long 类型 8个字节 64个bit
     * 符号位（1bit） + 时间戳（31bit） + 序列号（32bit）
     * @param keyPrefix
     * @return
     */
    public long nextId(String keyPrefix) {
        // 1.生成时间戳
        LocalDateTime now = LocalDateTime.now();
        long nowSecond = now.toEpochSecond(ZoneOffset.UTC);
        // ID的时间戳
        long timestamp = nowSecond - BEGIN_TIMESTAMP;

        // 2.生成序列号
        // 2.1.获取当前日期，精确到天
        String date = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));

        // 2.2.自增长
        // icr表示自增长，keyPrefix表示业务类型，一天一个key
        long count = stringRedisTemplate.opsForValue().increment("icr:" + keyPrefix + ":" + date);

        // 3.拼接并返回 (位运算)
        return timestamp << COUNT_BITS | count;
    }
}
