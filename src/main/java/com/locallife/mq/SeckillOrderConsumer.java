package com.locallife.mq;

import cn.hutool.json.JSONUtil;
import com.locallife.config.RabbitMQConfig;
import com.locallife.entity.VoucherOrder;
import com.locallife.service.IVoucherOrderService;
import com.locallife.utils.SimpleRedisLock;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.Map;

@Component
@Slf4j
public class SeckillOrderConsumer {

    @Resource
    private IVoucherOrderService voucherOrderService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @RabbitListener(queues = RabbitMQConfig.SECKILL_QUEUE)
    public void handleSeckillOrder(Message message, Channel channel) throws IOException {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        // 【修正】把消息体定义在最外层，全局可用，解决报错问题
        String json = new String(message.getBody());
        try {
            Map<String, Object> msgMap = JSONUtil.parseObj(json);
            Long voucherId = Long.valueOf(msgMap.get("voucherId").toString());
            Long userId = Long.valueOf(msgMap.get("userId").toString());
            Long orderId = Long.valueOf(msgMap.get("orderId").toString());

            // 2. 分布式锁防重
            SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
            if (!lock.tryLock(5)) {
                log.error("用户{}获取锁失败，消息重回队列重试", userId);
                // 临时异常：重回队列，不进死信
                channel.basicNack(deliveryTag, false, true);
                return;
            }

            try {
                // 3. 创建订单
                VoucherOrder order = new VoucherOrder();
                order.setId(orderId);
                order.setUserId(userId);
                order.setVoucherId(voucherId);
                voucherOrderService.createVoucherOrder(order);

                // 4. 成功确认
                channel.basicAck(deliveryTag, false);
                log.info("秒杀订单创建成功，orderId:{}", orderId);
            } finally {
                lock.unlock();
            }
        } catch (RuntimeException e) {
            log.error("业务下单失败：{}，消息：{}", e.getMessage(), json);
            // ✅ 业务失败：直接确认消息，然后删除消息，不进死信！
            channel.basicAck(deliveryTag, false);
        }
        // 👇 【重点】系统异常（数据库/网络/服务故障）
        catch (Exception e) {
            log.error("系统异常，不删除消息，消息进入死信：{}", json, e);
            // ❌ 系统异常：进死信队列
            channel.basicNack(deliveryTag, false, false);
        }
    }
}