package com.hmdp.service.impl;

import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TYPE_KEY;

@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result getByIconList() {

        // 1.在redis中间查询
        String key = CACHE_SHOP_TYPE_KEY;
        List<String> shopTypeList = new ArrayList<>();
        // range()中的 -1 表示最后一位
        // shopTypeList中存放的数据是[{...},{...},{...}...] 一个列表中有一个个json对象
        shopTypeList = stringRedisTemplate.opsForList().range(key,0,-1);

        // 2.判断是否缓存中了
        // 3.中了返回 （判断redis不空）
        if(!shopTypeList.isEmpty()) {
            List<ShopType> typeList = new ArrayList<>();
            for (String s : shopTypeList) {
                ShopType shopType = JSONUtil.toBean(s, ShopType.class);
                // shopType 是一个对象
                typeList.add(shopType);
            }
            return Result.ok(typeList);
        }

        // 4.redis未命中数据，从数据库中获取，根据ShopType对象的sort属性排序后存入typeList
        List<ShopType> typeList = query().orderByAsc("sort").list();
        // 5.数据库中如果不存在直接返回错误
        if(typeList.isEmpty()){
            return Result.fail("不存在分类");
        }

        for(ShopType shopType : typeList){
            String s = JSONUtil.toJsonStr(shopType);
            shopTypeList.add(s);
        }
        // 6.存在直接添加进缓存
        stringRedisTemplate.opsForList().rightPushAll(key, shopTypeList);
        return Result.ok(typeList);
    }
}
