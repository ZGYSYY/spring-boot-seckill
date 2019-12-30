package com.zgy.test;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMultiLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author ZGY
 * @date 2019/12/30 16:03
 * @description Test14App, 多共享锁对象 —Multi Shared Lock
 */
public class Test14App {

    private static final Logger LOGGER = LoggerFactory.getLogger(Test14App.class);

    @Test
    public void test() throws Exception {
        FakeLimitedResource resource = new FakeLimitedResource();
        CuratorFramework client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", 10000, 5000, new ExponentialBackoffRetry(5000, 3));
        client.start();

        // 创建一个可重入锁对象
        InterProcessLock lock = new InterProcessMutex(client, "/examples/locks");
        // 创建一个不可重入锁对象
        InterProcessLock lock2 = new InterProcessSemaphoreMutex(client, "/examples/locks2");
        // 创建多共享锁对象
        InterProcessLock lock3 = new InterProcessMultiLock(Arrays.asList(lock, lock2));

        if (!lock3.acquire(2000, TimeUnit.SECONDS)) {
            LOGGER.info("获取所有的锁失败！");
            return;
        }

        LOGGER.info("获取所有的锁成功！lock 是否获取到了锁：{}, lock2 是否获取到了锁：{}", lock.isAcquiredInThisProcess(), lock2.isAcquiredInThisProcess());

        try {
            // 使用共享资源
            resource.use();
        } finally {
            // 释放所有锁
            lock3.release();
            LOGGER.info("释放所有的锁成功！lock 是否获取到了锁：{}, lock2 是否获取到了锁：{}", lock.isAcquiredInThisProcess(), lock2.isAcquiredInThisProcess());
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
