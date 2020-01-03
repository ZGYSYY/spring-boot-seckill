package com.zgy.test;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryNTimes;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author ZGY
 * @date 2019/12/30 17:29
 * @description Test16App, 分布式long计数器—DistributedAtomicLong
 */
public class Test16App {

    private static final Logger LOGGER = LoggerFactory.getLogger(Test16App.class);

    @Test
    public void test() throws InterruptedException {
        CuratorFramework client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", 10000, 5000, new ExponentialBackoffRetry(2000, 3));
        client.start();

        ExecutorService executorService = Executors.newFixedThreadPool(5);
        for (int i = 0; i < 5; i++) {
            /**
             * 重试策略对象，参数解释如下：
             * 第一个参数：重试次数
             * 第二个参数：重试之间的睡眠时间
             */
            RetryNTimes retryNTimes = new RetryNTimes(2, 2);
            final DistributedAtomicLong atomicLong = new DistributedAtomicLong(client, "/examples/counter", retryNTimes);
            Callable<Void> task = () -> {
                try {
                    // 加 1
                    AtomicValue<Long> value = atomicLong.increment();
                    if (value.succeeded()) {
                        LOGGER.info("修改前的值为：{}， 修改后的值为：{}", value.preValue(), value.postValue());
                    } else {
                        LOGGER.info("修改失败");
                    }
                } catch (Exception e) {
                    LOGGER.error("程序出现异常！", e);
                }
                return null;
            };

            executorService.submit(task);
        }

        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.MINUTES);
    }
}
