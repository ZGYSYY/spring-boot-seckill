package com.zgy.test;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreV2;
import org.apache.curator.framework.recipes.locks.Lease;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author ZGY
 * @date 2019/12/30 14:55
 * @description Test13App, 信号量—Shared Semaphore
 */
public class Test13App {

    private static final Logger LOGGER = LoggerFactory.getLogger(Test13App.class);

    @Test
    public void test() throws Exception {
        FakeLimitedResource resource = new FakeLimitedResource();
        ExponentialBackoffRetry retry = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", 10000, 5000, retry);
        client.start();

        // 定义信号量变量，最大租约为 10
        InterProcessSemaphoreV2 semaphore = new InterProcessSemaphoreV2(client, "/examples/locks", 10);
        // 获取 5 个租约
        Collection<Lease> leases = semaphore.acquire(5);
        LOGGER.info("leases 租约数量为：{}", leases.size());

        // 获取 1 个租约
        Lease lease = semaphore.acquire();
        LOGGER.info("lease: [{}]", lease);

        // 使用共享资源
        resource.use();

        // 在 6 秒内获取 5 个租约，如果获取不了就返回 null
        Collection<Lease> leases2 = semaphore.acquire(5, 6, TimeUnit.SECONDS);
        if (leases2 != null) {
            LOGGER.info("leases2 租约数量为：{}", leases2.size());
        } else {
            LOGGER.info("在 6 秒内没有获取到 5 个租约，所以 leases2 为 null");
        }

        // 把使用的租约还给 semaphore
        semaphore.returnLease(lease);
        semaphore.returnAll(leases);
        if (leases2 != null) {
            semaphore.returnAll(leases2);
        }
    }

    /**
     * 共享资源
     */
    private class FakeLimitedResource {

        private final AtomicBoolean inUse = new AtomicBoolean(false);

        public void use() throws InterruptedException {
            // 如果设置值失败
            if (!inUse.compareAndSet(false, true)) {
                throw new RuntimeException("该资源的原始值不是 false，所以设置值失败！");
            }

            try {
                // 模拟程序复杂业务
                int seconds = new Random().nextInt(3);
                LOGGER.info("模拟程序复杂业务，耗时 {} 秒", seconds);
                TimeUnit.SECONDS.sleep(seconds);
            } finally {
                // 强制将 inUse 设置为 false
                inUse.set(false);
            }
        }
    }
}
