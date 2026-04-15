package com.locallife.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.json.JSONUtil;
import com.locallife.config.RabbitMQConfig;
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
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;


    @Resource
    private RabbitTemplate rabbitTemplate;

    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();

        // 获取订单id
        long orderId = redisIdWorker.nextId("order");

        // 1. 执行lua脚本（扣减Redis库存 + 判重）
        int execute = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString(),
                String.valueOf(orderId)
        ).intValue();

        // 2. 判断结果是否为0
        if (execute != 0) {
            return Result.fail(execute == 1 ? "库存不足" : "不能够重复下单");
        }

        // 3. Lua执行成功，发送RabbitMQ消息（异步下单）
        Map<String, Object> msg = new HashMap<>();
        msg.put("voucherId", voucherId);
        msg.put("userId", userId);
        msg.put("orderId", orderId);
        String jsonMsg = JSONUtil.toJsonStr(msg);
        rabbitTemplate.convertAndSend(
                RabbitMQConfig.SECKILL_EXCHANGE,
                RabbitMQConfig.SECKILL_ROUTING_KEY,
                jsonMsg
        );

        // 4. 返回订单id给前端
        return Result.ok(orderId);
    }

    /**
     * 真正下单的方法（由MQ消费者调用）
     * 注意：这里不再需要分布式锁，因为消费者中已经有锁了
     */
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();

        // 1. 判断是否重复下单
        Integer count = query().eq("user_id", userId)
                .eq("voucher_id", voucherId).count();

        if (count > 0) {
            log.error("用户 {} 已经购买过优惠券 {}", userId, voucherId);
            throw new RuntimeException("该用户已购买过此优惠券");
        }

        // 2. 扣减数据库库存（乐观锁）
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId)
                .gt("stock", 0)
                .update();

        if (!success) {
            log.error("优惠券 {} 库存不足", voucherId);
            throw new RuntimeException("库存不足");
        }

        // 3. 保存订单
        this.save(voucherOrder);
        log.info("订单创建成功: orderId={}, userId={}, voucherId={}",
                voucherOrder.getId(), userId, voucherId);
    }
}