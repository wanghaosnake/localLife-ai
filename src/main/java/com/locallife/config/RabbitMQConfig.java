package com.locallife.config;

import org.springframework.amqp.core.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQConfig {

    public static final String SECKILL_QUEUE = "seckill.order.queue";
    public static final String SECKILL_EXCHANGE = "seckill.exchange";
    public static final String SECKILL_ROUTING_KEY = "seckill.order";

    // ================== 原有：数据队列 ==================
    public static final String DATA_QUEUE = "data.order.queue";
    public static final String DATA_EXCHANGE = "data.exchange";
    public static final String DATA_ROUTING_KEY = "data.order";

    // ==================== 死信队列配置 ====================
    public static final String DEAD_EXCHANGE = "dead.exchange";
    public static final String DEAD_QUEUE = "dead.queue";
    public static final String DEAD_ROUTING_KEY = "dead";

    // 死信交换机
    @Bean
    public DirectExchange deadExchange() {
        return new DirectExchange(DEAD_EXCHANGE);
    }

    // 死信队列
    @Bean
    public Queue deadQueue() {
        return QueueBuilder.durable(DEAD_QUEUE).build();
    }

    // 死信绑定
    @Bean
    public Binding deadBinding() {
        return BindingBuilder.bind(deadQueue())
                .to(deadExchange())
                .with(DEAD_ROUTING_KEY);
    }

    // ================== 【核心修改】秒杀队列 + 绑定死信规则 ==================
    @Bean
    public Queue seckillQueue() {
        return QueueBuilder.durable(SECKILL_QUEUE)
                // 绑定死信交换机（关键！）
                .withArgument("x-dead-letter-exchange", DEAD_EXCHANGE)
                // 绑定死信路由键（关键！）
                .withArgument("x-dead-letter-routing-key", DEAD_ROUTING_KEY)
                .build();
    }

    // 秒杀交换机（不变）
    @Bean
    public DirectExchange seckillExchange() {
        return new DirectExchange(SECKILL_EXCHANGE, true, false);
    }

    // 秒杀绑定（不变）
    @Bean
    public Binding seckillBinding() {
        return BindingBuilder.bind(seckillQueue())
                .to(seckillExchange())
                .with(SECKILL_ROUTING_KEY);
    }

    // ================== 数据队列（不变，无需死信） ==================
    @Bean
    public Queue dataQueue() {
        return QueueBuilder.durable(DATA_QUEUE)
                // 核心：绑定死信交换机
                .withArgument("x-dead-letter-exchange", DEAD_EXCHANGE)
                // 核心：绑定死信路由键
                .withArgument("x-dead-letter-routing-key", DEAD_ROUTING_KEY)
                .build();
    }

    @Bean
    public DirectExchange dataExchange() {
        return new DirectExchange(DATA_EXCHANGE, true, false);
    }

    @Bean
    public Binding dataBinding() {
        return BindingBuilder.bind(dataQueue())
                .to(dataExchange())
                .with(DATA_ROUTING_KEY);
    }


}