package com.locallife.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.locallife.dto.Result;
import com.locallife.entity.VoucherOrder;
import com.locallife.mapper.VoucherOrderMapper;
import com.locallife.service.ISeckillVoucherService;
import com.locallife.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.locallife.utils.RedisIdWorker;
import com.locallife.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static{
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }


    private static final ExecutorService SECKILL_ORDER_EXECUTOR= Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

        private class VoucherOrderHandler implements Runnable{
        String queueName="stream.orders";
        @Override
        public void run() {
            while(true){
                try {
                    //1.获取消息队列中的订单信息XREADGROUP GROUP g1 c1 COUNT 1BLOCK 2000 STREAMS streams.order >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );

                    //2.判断消息获取是否成功
                    if(list==null || list.isEmpty()){
                        continue;
                    }
                    //3.解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);

                    //3.如果获取成功,可以下单
                    handleVoucherOrder(voucherOrder);

                    //4.ACK确认 SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常",e);
                    handlePendingList();
                }

            }
        }

            private void handlePendingList() {
                while(true){
                    try {
                        //1.获取pending-list中的订单信息XREADGROUP GROUP g1 c1 COUNT 1BLOCK 2000 STREAMS streams.order 0
                        List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                                Consumer.from("g1", "c1"),
                                StreamReadOptions.empty().count(1),
                                StreamOffset.create(queueName, ReadOffset.from("0"))
                        );

                        //2.判断消息获取是否成功
                        if(list==null || list.isEmpty()){
                            //获取失败,说明pending-list中没有异常信息,结束循环
                            break;
                        }
                        //3.解析消息中的订单信息
                        MapRecord<String, Object, Object> record = list.get(0);
                        Map<Object, Object> values = record.getValue();
                        VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);

                        //3.如果获取成功,可以下单
                        handleVoucherOrder(voucherOrder);

                        //4.ACK确认 SACK stream.orders g1 id
                        stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
                    } catch (Exception e) {
                        log.error("处理pending-list异常",e);

                    }

                }
            }
        }

//    private BlockingQueue<VoucherOrder> orderTasks=new ArrayBlockingQueue<VoucherOrder>(1024*1024);
//    private class VoucherOrderHandler implements Runnable{
//        @Override
//        public void run() {
//            while(true){
//                try {
//                    //1.获取队列中的订单信息
//                    VoucherOrder voucherOrder = orderTasks.take();
//
//                    //2.创建订单
//                    handleVoucherOrder(voucherOrder);
//                } catch (Exception e) {
//                    log.error("处理订单异常",e);
//                }
//
//            }
//        }
//    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        RLock lock = redissonClient.getLock("lock:order:" + userId);

        boolean isLock = lock.tryLock();
        if(!isLock){
            log.error("不允许重复下单");
            return;
        }

        try {
            //获取与事务有关的代理对象
            proxy.createVoucherOrder(voucherOrder);
        } finally{
//            simpleRedisLock.unlock();
            lock.unlock();
        }

    }


    @Resource
    private ISeckillVoucherService seckillVoucherService;


    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    private IVoucherOrderService proxy;
    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();

        //获取订单id
        long orderId = redisIdWorker.nextId("order");
        //1.执行lua脚本
        int execute = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString(),
                String.valueOf(orderId)
        ).intValue();

        //2.判断结果是否为0
        //2.1 不为0,代表没有购买资格
        if(execute!=0){
            return Result.fail(execute==1 ? "库存不足" : "不能够重复下单");
        }


        proxy = (IVoucherOrderService) AopContext.currentProxy();
        //3.返回订单id
        return Result.ok(orderId);

    }

//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//
//        if(voucher.getBeginTime().isAfter(LocalDateTime.now())){
//            return Result.fail("秒杀尚未开始!");
//        }
//
//        if(voucher.getEndTime().isBefore(LocalDateTime.now()))
//        {
//            return Result.fail("秒杀已经结束!");
//        }
//
//        if(voucher.getStock()<1){
//            return Result.fail("库存不足");
//        }
//
//        Long userId = UserHolder.getUser().getId();
//
//        //创建自定义的分布式锁对象
////        SimpleRedisLock simpleRedisLock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
//
//        RLock lock = redissonClient.getLock("lock:order:" + userId);
//
//        //自定义获取分布式锁
////        boolean isLock = simpleRedisLock.tryLock(1200);
//
//        boolean isLock = lock.tryLock();
//        if(!isLock){
//            //获取锁失败
//            return Result.fail("不允许重复下单!");
//        }
//
//        try {
//            //获取与事务有关的代理对象
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        } finally{
////            simpleRedisLock.unlock();
//            lock.unlock();
//        }
//
//    }

    @Transactional
    public  void createVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();


            Integer count = query().eq("user_id", userId)
                    .eq("voucher_id", voucherOrder.getVoucherId()).count();

            if (count > 0) {
                log.error("此用户已经购买过了,不能够在购买");
                return;
            }
            boolean success = seckillVoucherService.update()
                    .setSql("stock=stock-1")// set stock=stock-1
                    .eq("voucher_id", voucherOrder.getVoucherId()) //where id= ? ans stock = ?
                    .gt("stock", 0)
                    .update();


            if (!success) {
                log.error("库存不足!");
                return;

            }


            this.save(voucherOrder);


    }
}
