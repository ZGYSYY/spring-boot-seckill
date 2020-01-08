package com.zgy.test;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author ZGY
 * @date 2020/1/8 18:07
 * @description Test23App, 双栅栏—DistributedDoubleBarrier
 */
public class Test23App {

    private static final Logger LOGGER = LoggerFactory.getLogger(Test23App.class);

    @Test
    public void test() throws InterruptedException {
        CuratorFramework client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", 20000, 5000, new ExponentialBackoffRetry(5000, 3));
        client.start();

        // 创建线程池
        ExecutorService executorService = Executors.newFixedThreadPool(5);


        Random random = new Random();

        for (int i = 0; i < 5; i++) {
            // 创建栅栏对象
            final DistributedDoubleBarrier doubleBarrier = new DistributedDoubleBarrier(client, "/examples/barrier", 5);
            final int index = i;
            Callable<Void> task = () -> {
                TimeUnit.MILLISECONDS.sleep(random.nextInt(3));
                LOGGER.info("客户端 #" + index + "进入栅栏");
                doubleBarrier.enter();

                // 当所有线程都进入栅栏后，执行下面的代码
                LOGGER.info("执行代码");
                TimeUnit.SECONDS.sleep(random.nextInt(3));

                doubleBarrier.leave();
                LOGGER.info("客户端 #" + index + "离开栅栏");

                return null;
            };
            executorService.submit(task);
        }

        // 关闭线程池
        executorService.shutdown();

        // 和 executorService.shutdown() 组合使用，监控线程池是否已经关闭，如果线程池内之前提交的任务还没有完成，会一直监控到任务处理完成后。
        executorService.awaitTermination(10, TimeUnit.MINUTES);
    }
}
