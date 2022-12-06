package com.hmdp;

import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class HmDianPingApplicationTests {

    @Resource
    private ShopServiceImpl shopService;

    @Resource
    private RedissonClient redissonClient;

    /**
     * 调queryWithLogicalExpire前要先预热！！！
     */
    @Test
    void test() {
        shopService.saveShopRedis(1L,100L);
        shopService.saveShopRedis(2L,100L);
        shopService.saveShopRedis(3L,100L);
        shopService.saveShopRedis(4L,100L);
        shopService.saveShopRedis(5L,100L);

    }

    @Resource
    private RedisIdWorker redisIdWorker;

    private ExecutorService es = Executors.newFixedThreadPool(500);

    @Test
    void testIdWorker() throws InterruptedException {

        CountDownLatch latch = new CountDownLatch(300);

        Runnable task = () -> {
            for (int i = 0; i < 100; i++) {
                long id = redisIdWorker.nextId("order");
                System.out.println("id = " + id);
            }
            latch.countDown();
        };
        long start = System.currentTimeMillis();
        for (int i = 0; i < 300; i++) {
            es.submit(task);
        }
        latch.await();
        long end = System.currentTimeMillis();

        System.out.println(end - start);
    }

    /**
     * redissonClient
     * @throws Exception
     */
    @Test
    void testRedisson() throws Exception {
        RLock anyLock = redissonClient.getLock("anyLock");
        boolean isLock = anyLock.tryLock(1, 10, TimeUnit.SECONDS);
        if(isLock) {
            try {
                System.out.println("执行业务");
            } finally {
                anyLock.unlock();
            }
        }
    }


}
