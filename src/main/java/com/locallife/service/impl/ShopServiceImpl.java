package com.locallife.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.locallife.dto.Result;
import com.locallife.entity.RedisData;
import com.locallife.entity.Shop;
import com.locallife.mapper.ShopMapper;
import com.locallife.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.locallife.utils.CacheClient;
import com.locallife.utils.SystemConstants;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.locallife.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient   cacheClient;

    //自定义线程池:用于快速获得异步线程
    private static final ExecutorService CACHE_REBUILD_EXECUTOR= Executors.newFixedThreadPool(10);

    //缓存穿透
    @Override
    public Result queryById(Long id) throws InterruptedException {
        //缓存穿透
        //Result shop = queryWithPassThrough(id);
//        Shop shop = cacheClient.queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, id2 -> getById(id2), CACHE_SHOP_TTL, TimeUnit.MINUTES);

        //缓存击穿1:互斥锁方式
        //Shop shop = queryWithMutex(id);

        //缓存击穿2:逻辑过期
//        Shop shop = queryWithLogicalExpire(id);
        Shop shop = cacheClient.queryWithLogicalExpire(CACHE_SHOP_KEY, id, Shop.class, id2 -> getById(id2), 20L, TimeUnit.MINUTES);
        if(shop == null){
            return Result.fail("店铺不存在!");
        }

        return Result.ok(shop);

    }

    @Override
    @Transactional
    public Result updateShop(Shop shop) {
        Long id = shop.getId();
        if(ObjectUtil.isEmpty(id)){
            return Result.fail("店铺id不能够为空");
        }
        updateById(shop);

        stringRedisTemplate.delete(CACHE_SHOP_KEY + shop.getId());
        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        if(x==null || y==null){
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            return Result.ok(page.getRecords());
        }

        String key = SHOP_GEO_KEY + typeId;
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;


//        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo();
//                .search(
//                        key,
//                        GeoReference.fromCoordinate(x, y),
//                        new Distance(5000),
//                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
//                );

        if(null==null){
            return Result.ok(Collections.emptyList());
        }

        
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list =new ArrayList<>();
        List<Long>  ids= new ArrayList<>(list.size());
        Map<String,Distance> distanceMap=new HashMap<>(list.size());
        list.stream().skip(from).forEach(result->{
            String shopStr = result.getContent().getName();
            ids.add(Long.valueOf(shopStr));
            Distance distance = result.getDistance();
            distanceMap.put(shopStr,distance);
        });
        String idsStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id," + idsStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }

        return Result.ok(shops);

    }

    //缓存击穿1:互斥锁方式
    public Shop queryWithMutex(Long id) throws InterruptedException {

        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);

        if(StrUtil.isNotBlank(shopJson)){
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }


        //此时的shopJson要么是null,要么是为了解决缓存穿透的而存储的""
        if(shopJson!=null) {
            return null;
        }

        Shop shop = null;
        try {
            boolean isLock = this.tryLock(LOCK_SHOP_KEY);

            if(!isLock){
                Thread.sleep(50);
                return this.queryWithMutex(id);
            }

            shop = getById(id);
            //模拟查询数据库重建缓存时,其他线程是否页获取锁
            Thread.sleep(200);
            if (ObjectUtil.isEmpty(shop)) {
                stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, "",CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }


            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            this.unlock(LOCK_SHOP_KEY);
        }


        return shop;

    }

    //缓存击穿2:逻辑过期方式
    public Shop queryWithLogicalExpire(Long id) throws InterruptedException {
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);

        if(StrUtil.isBlank(shopJson)){

            return null;
        }

        //命中需要判断逻辑过期时间是否过期
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();

        if(expireTime.isAfter(LocalDateTime.now())){
            return shop;
        }

        String key = LOCK_SHOP_KEY + id;
        boolean isLock = this.tryLock(key);
        if(isLock){
            //获取锁成功,开启独立线程,实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(()->{
                try {
                    this.saveShop2Redis(id,20L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    this.unlock(key);
                }
            });
        }

        return shop;





    }

    //逻辑过期第一步:数据预热,先存入redis当中
    public  void saveShop2Redis(Long id,Long expireSeconds) throws InterruptedException {
        //1.查询店铺数据
        Shop shop = getById(id);
        Thread.sleep(200);
        //2.封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));

        //3.写入redis当中
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id, JSONUtil.toJsonStr(redisData));
    }

    //缓存穿透
    public Result queryWithPassThrough(Long id){
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);

        if(StrUtil.isNotBlank(shopJson)){
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return Result.ok(shop);
        }

        //此时的shopJson要么是null,要么是为了解决缓存穿透的而存储的""
        if(shopJson!=null) {
            return Result.fail("解决缓存穿透,店铺信息不存在!");
        }

        Shop shop = getById(id);
        if (ObjectUtil.isEmpty(shop)) {
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, "",CACHE_NULL_TTL, TimeUnit.MINUTES);
            return Result.fail("发生缓存穿透,店铺信息不存在!");
        }


        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        return Result.ok(shop);
    }

    //实现人为实现分布式锁setnx key value
    private boolean tryLock(String key){
        Boolean b = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(b);

    }

    //释放人为分布式锁delete key
    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }
}
