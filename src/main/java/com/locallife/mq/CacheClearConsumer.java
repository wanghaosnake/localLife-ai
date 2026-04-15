package com.locallife.mq;

import cn.hutool.json.JSONUtil;
import com.locallife.config.RabbitMQConfig;
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
public class CacheClearConsumer {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    private static final String CACHE_SHOP_KEY = "cache:shop:";

    @RabbitListener(queues = RabbitMQConfig.DATA_QUEUE)
    public void handleCacheClear(Message message, Channel channel) throws IOException {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        String msgBody = new String(message.getBody());

        try {
            // 1. 解析消息
            Map<String, Object> msgMap = JSONUtil.parseObj(msgBody);
            Long shopId = Long.valueOf(msgMap.get("shopId").toString());

            // 2. 执行缓存清理
            String cacheKey = CACHE_SHOP_KEY + shopId;
            Boolean deleted = stringRedisTemplate.delete(cacheKey);

            // 3. 日志输出
            if (Boolean.TRUE.equals(deleted)) {
                log.info("缓存清理成功: shopId={}", shopId);
            } else {
                log.warn("缓存不存在/已清理: shopId={}", shopId);
            }

            // 4. 业务正常 → 手动ACK
            channel.basicAck(deliveryTag, false);

        } catch (Exception e) {
            log.error("缓存清理失败【系统异常】，消息进入死信队列，内容：{}", msgBody, e);
            // 系统异常（Redis宕机/网络故障）→ 拒绝消息，自动进死信
            channel.basicNack(deliveryTag, false, false);
        }
    }
}