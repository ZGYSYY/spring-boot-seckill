package com.zgy.test;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author ZGY
 * @date 2019/12/30 13:59
 * @description Test12App, 可重入读写锁 Shared Reentrant Read Write Lock
 */
public class Test12App {

    private static final Logger LOGGER = LoggerFactory.getLogger(Test12App.class);

    @Test
    public void test() throws InterruptedException {
        ExponentialBackoffRetry retry = new ExponentialBackoffRetry(1000, 3);

        final FakeLimitedResource resource = new FakeLimitedResource();
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        for (int i = 0; i < 5; i++) {
            final int index = i;
            Runnable task = () -> {
                final CuratorFramework client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", 5000, 5000, retry);
                try {
                    client.start();
                    ReentrantReadWriteLockDemo demo = new ReentrantReadWriteLockDemo(client, "/examples/locks", resource, "客户端" + index);
                    demo.doWork(5, TimeUnit.SECONDS);
                } finally {
                    CloseableUtils.closeQuietly(client);
                }
            };
            executorService.execute(task);
        }

        executorService.shutdown();

        executorService.awaitTermination(5, TimeUnit.MINUTES);
    }

    private class ReentrantReadWriteLockDemo {

        private final InterProcessReadWriteLock lock;
        private final InterProcessMutex readLock;
        private final InterProcessMutex writeLock;
        private final FakeLimitedResource resource;
        private final String clientName;

        public ReentrantReadWriteLockDemo(CuratorFramework client, String lockPath, FakeLimitedResource resource, String clientName) {
            this.lock = new InterProcessReadWriteLock(client, lockPath);
            this.readLock = this.lock.readLock();
            this.writeLock = this.lock.writeLock();
            this.resource = resource;
            this.clientName = clientName;
        }

        /**
         * 业务功能
         * @param time
         * @param unit
         */
        public void doWork(long time, TimeUnit unit) {
            try {
                if (!this.writeLock.acquire(time, unit)) {
                    LOGGER.info("{} 不能获取到写锁", this.clientName);
                }
                LOGGER.info("{} 已得到写锁", this.clientName);
                if (!this.readLock.acquire(time, unit)) {
                    LOGGER.info("{} 不能获取到读锁", this.clientName);
                }
                LOGGER.info("{} 已得到读锁", this.clientName);

                LOGGER.info("开始使用共享资源");
                resource.use();
                LOGGER.info("共享资源使用完毕");
            } catch (Exception e) {
                LOGGER.error("public void doWork(long time, TimeUnit unit) 方法发生异常！", e);
            } finally {
                try {
                    LOGGER.info("业务处理完毕，开始释放锁");
                    if (this.readLock.isAcquiredInThisProcess()) {
                        this.readLock.release();
                    }
                    if (this.writeLock.isAcquiredInThisProcess()) {
                        this.writeLock.release();
                    }
                    LOGGER.info("锁释放成功！");
                } catch (Exception e) {
                    LOGGER.error("public void doWork(long time, TimeUnit unit) 方法释放锁发生异常！", e);
                }
            }
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
