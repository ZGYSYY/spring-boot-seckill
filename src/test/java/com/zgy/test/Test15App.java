package com.zgy.test;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.framework.recipes.shared.SharedCountListener;
import org.apache.curator.framework.recipes.shared.SharedCountReader;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author ZGY
 * @date 2019/12/30 16:56
 * @description Test15App, 分布式int计数器—SharedCount
 */
public class Test15App {

    private static final Logger LOGGER = LoggerFactory.getLogger(Test15App.class);

    @Test
    public void test() throws Exception {
        final Random random = new Random();
        SharedCounterDemo demo = new SharedCounterDemo();
        CuratorFramework client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", 10000, 5000, new ExponentialBackoffRetry(2000, 3));
        client.start();
        // 创建计数器对象
        SharedCount sharedCount = new SharedCount(client, "/examples/counter", 0);
        // 给计数器对象添加监听器
        sharedCount.addListener(demo);
        // 启动计数器
        sharedCount.start();

        List<SharedCount> sharedCounts = new ArrayList<>();
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        for (int i = 0; i < 5; i++) {
            final SharedCount SharedCount = new SharedCount(client, "/examples/counter", 0);
            sharedCounts.add(sharedCount);
            Callable<Void> task = () -> {
                SharedCount.start();
                TimeUnit.SECONDS.sleep(random.nextInt(10));
                boolean b = SharedCount.trySetCount(sharedCount.getVersionedValue(), random.nextInt(100));
                while (!b) {
                    b = SharedCount.trySetCount(sharedCount.getVersionedValue(), random.nextInt(100));
                }
                LOGGER.info("修改数据，b: [{}]", b);
                return null;
            };
            executorService.submit(task);
        }

        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.MINUTES);

        // 关闭计数器
        for (SharedCount count : sharedCounts) {
            count.close();
        }

        sharedCount.close();
    }

    private class SharedCounterDemo implements SharedCountListener {
        /**
         * 数字更改时触发
         * @param sharedCount
         * @param newCount
         * @throws Exception
         */
        @Override
        public void countHasChanged(SharedCountReader sharedCount, int newCount) throws Exception {
            LOGGER.info("数据被修改为：{}", newCount);
        }

        /**
         * 状态更改时触发
         * @param client
         * @param newState
         */
        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState) {
            LOGGER.info("状态被修改为了：{}", newState);
        }
    }
}
