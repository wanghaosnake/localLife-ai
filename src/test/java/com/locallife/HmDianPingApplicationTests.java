package com.locallife;

import com.locallife.entity.Shop;
import com.locallife.service.IShopService;
import com.locallife.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@SpringBootTest
class HmDianPingApplicationTests {

    @Resource
    private IShopService shopService;

    @Resource
    private RedisIdWorker redisIdWorker;

    private ExecutorService es= Executors.newFixedThreadPool(500);

    @Resource
    private StringRedisTemplate stringRedisTemplate;



    @Test
    void testIdWork() throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(300);
            Runnable task=()->{
                for(int i=0;i<100;i++){
                    long id = redisIdWorker.nextId("order");
                    System.out.println("id="+id);
                }
                countDownLatch.countDown();
            };
        long begin = System.currentTimeMillis();
        for(int i=0;i<300;i++){
                es.submit(task);
            }
        countDownLatch.await();
        long end = System.currentTimeMillis();
        System.out.println("time:"+(end-begin));
    }

    @Test
    void loadShopData(){
        List<Shop> list = shopService.list();

        Map<Long,List<Shop>> map =list.stream().collect(Collectors.groupingBy(shop->shop.getTypeId()));
        for(Map.Entry<Long,List<Shop>> entry:map.entrySet()){
            Long typeId = entry.getKey();
            String key="shop:geo:"+typeId;
            List<Shop> value = entry.getValue();

            for (Shop shop : value) {
                stringRedisTemplate.opsForGeo().add(key,new Point(shop.getX(),shop.getY()),shop.getId().toString());
            }
        }
    }


}
