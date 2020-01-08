package com.zgy.test;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
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
 * @date 2020/1/8 17:30
 * @description Test22App, DistributedBarrier
 */
public class Test22App {

    private static final Logger LOGGER = LoggerFactory.getLogger(Test22App.class);

    @Test
    public void test() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", 20000, 5000, new ExponentialBackoffRetry(5000, 3));
        client.start();

        // 创建线程池
        ExecutorService executorService = Executors.newFixedThreadPool(5);

        // 创建栅栏对象
        final DistributedBarrier barrier = new DistributedBarrier(client, "/examples/barrier");
        // 设置栅栏，在该栅栏上的所有线程都将阻塞
        barrier.setBarrier();

        Random random = new Random();

        for (int i = 0; i < 5; i++) {
            final int index = i;
            Callable<Void> task = () -> {
                TimeUnit.MILLISECONDS.sleep(random.nextInt(3));
                LOGGER.info("客户端 #" + index + "准备数据");

                // 告诉栅栏对象，数据准备完毕
                try {
                    barrier.waitOnBarrier();
                } catch (Exception e) {
                    LOGGER.error("程序出现异常！", e);
                }
                // 栅栏对象放行后，执行
                LOGGER.info("开始处理数据！");
                return null;
            };
            executorService.submit(task);
        }

        TimeUnit.SECONDS.sleep(5);

        LOGGER.info("当所有线程数据准备完毕后，开始放行！");

        // 放行栅栏
        barrier.removeBarrier();

        // 关闭线程池
        executorService.shutdown();

        // 和 executorService.shutdown() 组合使用，监控线程池是否已经关闭，如果线程池内之前提交的任务还没有完成，会一直监控到任务处理完成后。
        executorService.awaitTermination(10, TimeUnit.MINUTES);
    }
}
