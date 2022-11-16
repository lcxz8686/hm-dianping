package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;

/**
 * 1. 秒杀开始|结束。未开始 or 已结束则无法下单
 * 2. 库存是否充足，不足无法下单
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Override
    @Transactional
    public Result seckillVoucher(Long voucherId) {

        // 1. 查询优惠券
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        LocalDateTime nowTime = LocalDateTime.now();

        // 2. 判断秒杀是否开始
        if (nowTime.isBefore(voucher.getBeginTime())) {
            return Result.fail("活动未开始！");
        }

        // 3. 判断秒杀是否结束
        if (nowTime.isAfter(voucher.getEndTime())) {
            return Result.fail("活动已结束！");
        }

        // 4. 判断库存
        if (voucher.getStock() < 1) {
            return Result.fail("已买完！");
        }

        // 5. 减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId).update();

        if (!success) {
            return Result.fail("库存不足");
        }

        // 6. 创建订单
        VoucherOrder voucherOrder = new VoucherOrder();

        // 6.1 订单id
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        // 6.2 用户id
        Long userId = UserHolder.getUser().getId();
        voucherOrder.setUserId(userId);
        // 6.3代金券id
        voucherOrder.setVoucherId(voucherId);
        save(voucherOrder);

        // 7. 返回订单
        return Result.ok(orderId);
    }
}
