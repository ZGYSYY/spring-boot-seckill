package com.itstyle.seckill.queue.redis;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;
import java.util.concurrent.*;

/**
 * @author ZGY
 */
@Configuration
public class RedisSubListenerConfig {

    /**
     * 初始化监听器
     * @param connectionFactory
     * @param listenerAdapter
     * @return
     */
    @Bean
    RedisMessageListenerContainer container(RedisConnectionFactory connectionFactory, MessageListenerAdapter listenerAdapter) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.addMessageListener(listenerAdapter, new PatternTopic("seckill"));
        /**
         * 如果不定义线程池，每一次消费都会创建一个线程，如果业务层面不做限制，就会导致秒杀超卖
         */
        ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("redis-listener-pool-%d").build();
        /*
         * corePoolSize 不要设置为 1，如果设置为 1，订阅线程和消费线程公用一个线程池，而该线程池只有一个核心线程。订阅线程在系统启动后
         * 就已经开始占用了核心线程，导致消费线程获取不了执行权。所以不会被消费。
         * 以下是源代码分析：
         *
         * ```java
         * RedisMessageListenerContainer 类的如下方法
         * public void afterPropertiesSet() {
         * 		if (taskExecutor == null) {
         * 			manageExecutor = true;
         * 			taskExecutor = createDefaultTaskExecutor();
         *                }
         *
         * 		if (subscriptionExecutor == null) {
         * 			subscriptionExecutor = taskExecutor;
         *        }
         *
         * 		initialized = true;
         * }
         * ```
         *
         * 如果非要设置为 1，就要设置添加如下点，定义一个订阅线程池
         * container.setSubscriptionExecutor(Executors.newFixedThreadPool(1));
         */
        ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 2, 5L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(1000), factory);
        container.setSubscriptionExecutor(Executors.newFixedThreadPool(1));
        container.setTaskExecutor(executor);
        return container;
    }

    /**
     * 利用反射来创建监听到消息之后的执行方法
     * @param redisReceiver
     * @return
     */
    @Bean
    MessageListenerAdapter listenerAdapter(RedisConsumer redisReceiver) {
        return new MessageListenerAdapter(redisReceiver, "receiveMessage");
    }

    /**
     * 使用默认的工厂初始化redis操作模板
     * @param connectionFactory
     * @return
     */
    @Bean
    StringRedisTemplate template(RedisConnectionFactory connectionFactory) {
        return new StringRedisTemplate(connectionFactory);
    }
}