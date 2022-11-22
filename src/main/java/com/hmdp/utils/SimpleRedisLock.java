package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * redis的分布式锁
 * 实现ILock接口
 */
public class SimpleRedisLock implements ILock {

    // 不同的业务有不同的锁名称
    private String name;
    private StringRedisTemplate stringRedisTemplate;
    private static final String KEY_PREFIX = "lock:";
    private static final String ID_PREFIX = UUID.randomUUID().toString(true) + "-";
    // DefaultRedisScript，
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;

    public SimpleRedisLock(String name, StringRedisTemplate stringRedisTemplate) {
        this.name = name;
        this.stringRedisTemplate = stringRedisTemplate;
    }

    // 初始化 UNLOCK_SCRIPT，用静态代码块的方式，一加载SimpleRedisLock有会加载unlock.lua
    // 避免每次调unLock() 才去加载，提升性能！！！
    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        // setLocation() 设置脚本位置
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
        // 返回值类型
        UNLOCK_SCRIPT.setResultType(Long.class);
    }

    /**
     * 获取锁
     */
    @Override
    public boolean tryLock(long timeoutSec) {
        // 获取线程标示
        String threadId = ID_PREFIX + Thread.currentThread().getId();
        // 获取锁
        // set lock thread1 nx ex 10
        // nx : setIfAbsent(如果不存在) , ex : timeoutSec(秒)
        Boolean success = stringRedisTemplate.opsForValue()
                .setIfAbsent(KEY_PREFIX + name, threadId, timeoutSec, TimeUnit.SECONDS);
        // 自动拆箱（Boolean -> boolean）！！！可能有风险
        return Boolean.TRUE.equals(success);
    }

    /**
     * 解决判断（锁标识、释放锁）这两个动作，之间产生阻塞！！！
     * JVM的 FULL GC
     * 要让这两个动作具有原子性
     */
    @Override
    public void unlock() {
        // 调用lua脚本
        stringRedisTemplate.execute(
                UNLOCK_SCRIPT,  // 脚本
                Collections.singletonList(KEY_PREFIX + name),  // 集合中只有一个字符串
                ID_PREFIX + Thread.currentThread().getId());  // args 其它参数
    }

    /**
     * 释放锁
     * 进阶版本
     */
//    @Override
//    public void unlock() {
//        // 获取线程标示
//        String threadId = ID_PREFIX + Thread.currentThread().getId();
//        // 获取锁中的标示
//        String id = stringRedisTemplate.opsForValue().get(KEY_PREFIX + name);
//        // 判断标示是否一致
//        if(threadId.equals(id)) {
//            // 释放锁
//            stringRedisTemplate.delete(KEY_PREFIX + name);
//        }
//    }

    /**
     *  初级版本！
      */
//    @Override
//    public void unlock() {
//        stringRedisTemplate.delete(KEY_PREFIX + name);
//    }
}
