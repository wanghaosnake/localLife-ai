package com.locallife.mq;

import cn.hutool.json.JSONUtil;
import com.locallife.config.RabbitMQConfig;
import com.locallife.entity.FailedOrder;
import com.locallife.mapper.FailedOrderMapper;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.IOException;
import java.time.LocalDateTime;

/**
 * 死信队列消费者：处理系统异常的秒杀订单
 * 核心：记录日志 + 持久化到数据库 + 人工后续排查
 */
@Component
@Slf4j
public class DeadLetterConsumer {

    @Resource
    private FailedOrderMapper failedOrderMapper;

    @RabbitListener(queues = RabbitMQConfig.DEAD_QUEUE)
    public void handleDeadLetter(Message message, Channel channel) throws IOException {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        String msgBody = new String(message.getBody());

        try {
            log.error("===== 死信队列接收异常订单 =====");
            log.error("消息内容：{}", msgBody);

            // 1. 构建失败订单对象，存入数据库
            FailedOrder failedOrder = new FailedOrder();
            failedOrder.setMsgContent(msgBody);
            failedOrder.setErrorInfo("秒杀订单消费异常，进入死信队列");
            failedOrder.setCreateTime(LocalDateTime.now());
            failedOrder.setStatus(0); // 未处理

            // 2. 保存到数据库
            failedOrderMapper.insert(failedOrder);
            log.info("死信消息已保存至数据库失败表");

            // 3. 手动确认消息，删除死信
            channel.basicAck(deliveryTag, false);

        } catch (Exception e) {
            log.error("死信消息处理/入库失败", e);
            // 入库失败：拒绝消息，重回队列（保证不丢失）
            channel.basicNack(deliveryTag, false, true);
        }
    }
}