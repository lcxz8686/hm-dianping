package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.RedisData;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    // 创建线程池 (逻辑过期解决缓存击穿问题)
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    /**
     * 获取锁
     * @param key
     * @return
     */
    // 这里的lock不是一个真正的线程锁，
    // 而是通过redis特性模拟的！！！
    private boolean tryLock(String key) {
        // setnx 就是 setIfAbsent 如果存在
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.MINUTES);
        // 装箱是将值类型装换成引用类型的过程；拆箱就是将引用类型转换成值类型的过程
        // 不要直接返回flag，可能为null
        return BooleanUtil.isTrue(flag);
    }

    /**
     * 释放锁
     * @param key
     */
    private void unLock(String key) {
        stringRedisTemplate.delete(key);
    }

    @Override
    public Result queryById(Long id) {

        // 缓存穿透
        // Shop shop = queryWithPassThrough(id);

        // 1.互斥锁解决缓存击穿问题
        // Shop shop = queryWithMutex(id);

        // 2.逻辑过期解决缓存击穿问题
        Shop shop = queryWithLogicalExpire(id);

        if (shop == null) {
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }

    /**
     * 缓存穿透解决方案 queryWithPassThrough()
     * @param id
     * @return
     */
    public Shop queryWithPassThrough(Long id) {
        // 1.从redis查询商铺缓存
        String key = CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        // 2.判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            return JSONUtil.toBean(shopJson, Shop.class);
        }

        // 判断空值
        if (shopJson != null) {
            // 返回一个错误信息
            return null;
        }

        // 4.不存在，根据id查询数据库
        Shop shop = getById(id);

        // 5.不存在，返回错误
        if (shop == null) {
            // 防止穿透问题，将空值写入redis!!!
            stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
            return null;
        }

        // 6.存在，写入Redis
        // 把shop转换成为JSON形式写入Redis
        // 同时添加超时时间
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        return shop;
    }

    /**
     * 互斥锁解决缓存击穿 queryWithMutex()
     * @param id
     * @return
     */
    public Shop queryWithMutex(Long id) {
        // 1.从redis查询商铺缓存
        String key = CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        // 2.判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            return JSONUtil.toBean(shopJson, Shop.class);
        }

        // 判断空值
        if (shopJson != null) {
            // 返回一个错误信息
            return null;
        }

        String lockKey = "lock:shop:" + id;
        Shop shop = null;
        try {
            // 4.实现缓存重建
            // 4.1获取互斥锁
            boolean isLock = tryLock(lockKey);

            // 4.2判断是否成功
            if (!isLock) {
                // 4.3失败，则休眠并重试
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            // 4.4成功，根据id查询数据库
            shop = getById(id);

            // 模拟延迟
            Thread.sleep(200);

            // 5.不存在，返回错误
            if (shop == null) {
                stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
                return null;
            }

            // 6.存在，写入redis
            stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL,TimeUnit.MINUTES);

        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        } finally {
            // 7.释放锁
            unLock(lockKey);
        }

        // 8.返回
        return shop;
    }

    /**
     * 逻辑过期解决缓存击穿问题 queryWithLogicalExpire()
     * 测试前要先缓存预热一下！不然 data 与 expireTime 的缓存值是null！
     * @param id
     * @return
     */
    public Shop queryWithLogicalExpire(Long id) {
        // 1.从redis查询商铺缓存
        String key = CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        // 2.判断是否存在
        if (StrUtil.isBlank(shopJson)) {
            return null;
        }

        // 4.命中，需要将json反序列化为对象
        // redisData没有数据
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();

        // 5.判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            // 5.1未过期，直接返回店铺信息
            return shop;
        }

        // 5.2已过期，需要缓存重建
        // 6.缓存重建
        // 6.1.获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        boolean islock = tryLock(lockKey);
        // 6.2.判断是否获取互斥锁成功
        if (islock) {
            // 6.3.成功，开启独立线程，实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit( () -> {
                try {
                    // 重建缓存，过期时间为20L
                    saveShopRedis(id,20L);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                } finally {
                    unLock(lockKey);
                }
            });
        }
        // 6.4.返回过期店铺信息
        return shop;
    }

    /**
     * 重建缓存,先缓存预热一下，否则queryWithLogicalExpire() 的expire为null
     * @param id
     * @param expireSeconds
     */
    public void saveShopRedis(Long id, Long expireSeconds) {
        // 1.查询店铺数据
        Shop shop = getById(id);
        // 2.封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));  // 过期时间
        // 3.写入redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }


    @Override
    @Transactional
    public Result update(Shop shop) {
        System.out.println("up");
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id不能为空！");
        }
        // 1.更新数据库
        updateById(shop);

        // 2.删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);

        return Result.ok();
    }
}
