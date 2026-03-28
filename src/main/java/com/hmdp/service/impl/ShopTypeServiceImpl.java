package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_LIST_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryTypeList() {

        String shopList = stringRedisTemplate.opsForValue().get(CACHE_SHOP_LIST_KEY);

        if(ObjectUtil.isNotEmpty(shopList)){
            return Result.ok(JSONUtil.toList(JSONUtil.parseArray(shopList),ShopType.class));
        }

        List<ShopType> shopTypeList = query().orderByAsc("sort").list();
        if(ObjectUtil.isNotEmpty(shopTypeList)){
            String shopStr = JSONUtil.toJsonStr(shopTypeList);
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_LIST_KEY, shopStr);
            return Result.ok(shopTypeList);
        }

        return Result.fail("查询商铺列表出现错误!");
    }
}
