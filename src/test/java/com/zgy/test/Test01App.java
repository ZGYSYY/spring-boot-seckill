package com.zgy.test;

import com.google.common.util.concurrent.RateLimiter;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @author ZGY
 * @date 2019/12/17 16:21
 * @description Test01App，RateLimiter 的使用案例
 */
public class Test01App {

    private static final Logger LOGGER = LoggerFactory.getLogger(Test01App.class);

    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss SSS");

    /**
     * RateLimiter 基本使用
     */
    @Test
    public void test() {

        // 系统启动后在令牌桶中每秒钟创建 2 个令牌。
        final RateLimiter rateLimiter = RateLimiter.create(2);

        // 创建 20 个线程
        Thread[] threads = new Thread[20];
        for (int i=0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                // 从令牌桶中获取一个令牌
                rateLimiter.acquire(1);
                LOGGER.info("线程{}获取到了令牌，获取令牌的时间是：{}", Thread.currentThread().getName(), LocalDateTime.now().format(dateTimeFormatter));
            });
        }

        // 运行线程
        for (Thread thread : threads) {
            thread.start();
        }

        for (;;);
    }

    /**
     * RateLimiter 预消费测试
     */
    @Test
    public void test2() {
        // 系统启动后在令牌桶中每秒钟创建 5 个令牌。
        RateLimiter rateLimiter = RateLimiter.create(5);
        LOGGER.info("获取1个令牌开始，时间为: {}", LocalDateTime.now().format(dateTimeFormatter));
        double cost = rateLimiter.acquire(1);
        LOGGER.info("获取1个令牌结束，时间为: {}，耗时：{} ms", LocalDateTime.now().format(dateTimeFormatter), cost);

        LOGGER.info("获取5个令牌开始，时间为: {}", LocalDateTime.now().format(dateTimeFormatter));
        cost = rateLimiter.acquire(5);
        LOGGER.info("获取5个令牌结束，时间为: {}，耗时：{} ms", LocalDateTime.now().format(dateTimeFormatter), cost);

        LOGGER.info("获取3个令牌开始，时间为: {}", LocalDateTime.now().format(dateTimeFormatter));
        cost = rateLimiter.acquire(3);
        LOGGER.info("获取3个令牌结束，时间为: {}，耗时：{} ms", LocalDateTime.now().format(dateTimeFormatter), cost);
    }
}
