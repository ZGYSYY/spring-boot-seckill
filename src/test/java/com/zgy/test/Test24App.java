package com.zgy.test;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.*;

/**
 * @author ZGY
 * @date 2020/1/9 16:38
 * @description Test24App, 线程池测试
 */
public class Test24App {

    private static final Logger LOGGER = LoggerFactory.getLogger(Test24App.class);

    /**
     * 创建一个核心线程为1，最大线程为1，队列长度为1000的线程池，执行任务。
     */
    @Test
    public void test() throws InterruptedException {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 5L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(1000));
        Random random = new Random();

        for (int i = 0; i < 5; i++) {
            final int index = i;
            Runnable task = () -> {
                try {
                    TimeUnit.MILLISECONDS.sleep(random.nextInt(10));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                LOGGER.info("===============> 任务：{} 执行", index);
            };
            executor.execute(task);
        }

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.MINUTES);
        LOGGER.info("程序执行完毕！");
    }
}
