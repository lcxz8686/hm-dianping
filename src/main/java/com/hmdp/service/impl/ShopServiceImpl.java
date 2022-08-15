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

    // 创建线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public Shop queryWithLogicalExpire(Long id) {
        // 1.从redis查询商铺缓存
        String key = CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        // 2.判断是否存在
        if (StrUtil.isBlank(shopJson)) {
            return null;
        }

        // 4.命中，需要将json反序列化为对象
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
                    // 查询店铺数据
                    Shop shopTemp = getById(id);
                    // 封装逻辑过期时间
                    RedisData redisDataTemp = new RedisData();
                    redisDataTemp.setData(shopTemp);
                    redisDataTemp.setExpireTime(LocalDateTime.now().plusSeconds(20L));
                    // 写入redis
                    stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisDataTemp));
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                } finally {
                    unlock(lockKey);
                }
            });
        }
        // 6.4.返回过期店铺信息
        return shop;
    }

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
            unlock(lockKey);
        }

        // 8.返回
        return shop;
    }

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


    // 这里的lock不是一个真正的线程锁，
    // 而是通过redis特性模拟的！！！
    private boolean tryLock(String key) {
        // setnx 就是 setIfAbsent
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.MINUTES);
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key) {
        stringRedisTemplate.delete(key);
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
