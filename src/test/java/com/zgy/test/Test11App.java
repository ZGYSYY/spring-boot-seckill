package com.zgy.test;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
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
 * @date 2019/12/27 14:43
 * @description Test11App, Curator 高级特性——不可重入共享锁 示例代码
 */
public class Test11App {

    private static final Logger LOGGER = LoggerFactory.getLogger(Test11App.class);

    @Test
    public void test() throws InterruptedException {
        // 创建共享资源对象
        final FakeLimitedResource resource = new FakeLimitedResource();
        // 创建线程池
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        // 重试策略对象
        final ExponentialBackoffRetry retry = new ExponentialBackoffRetry(1000, 3);

        for (int i = 0; i < 5; i++) {
            final int index = i;
            Runnable task = () -> {
                CuratorFramework client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", 5000, 5000, retry);
                try {
                    client.start();
                    InterProcessSemaphoreMutexDemo mutexDemo = new InterProcessSemaphoreMutexDemo(client, "/examples/locks", resource, "客户端" + index);
                    for (int j = 0; j < 5; j++) {
                        mutexDemo.doWork(5, TimeUnit.SECONDS);
                    }
                } finally {
                    CloseableUtils.closeQuietly(client);
                }
            };

            // 交给线程池执行
            executorService.submit(task);
        }

        // 当线程池中的所有任务执行完后，关闭线程池
        executorService.shutdown();
        // 等待除主线程外其他线程都执行完毕
        executorService.awaitTermination(10, TimeUnit.MINUTES);
    }

    private class InterProcessSemaphoreMutexDemo {

        /**
         * 不可重入贡献锁对象
         */
        private InterProcessSemaphoreMutex lock;
        /**
         * 共享资源对象
         */
        private FakeLimitedResource resource;
        /**
         * 客户端名称
         */
        private String clientName;

        /**
         * @param client 连接 zookeeper 的客户端对象
         * @param lockPath 锁路径
         * @param resource 共享资源对象
         * @param clientName 客户名称
         */
        public InterProcessSemaphoreMutexDemo(CuratorFramework client, String lockPath, FakeLimitedResource resource, String clientName) {
            this.lock = new InterProcessSemaphoreMutex(client, lockPath);
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
                LOGGER.info("{} 第一次获取锁", this.clientName);
                if (!lock.acquire(time, unit)) {
                    LOGGER.info("{} 第一次没有获取到锁！", this.clientName);
                    return;
                }

                // 这个示例代码是不可重入锁，所以这里会获取不了锁
                LOGGER.info("{} 第二次获取锁", this.clientName);
                if (!lock.acquire(time, TimeUnit.SECONDS)) {
                    LOGGER.info("{} 第二次没有获取到锁！", this.clientName);
                    return;
                }

                LOGGER.info("{} 获取到了锁，马上开始使用资源！", this.clientName);
                this.resource.use();
            } catch (Exception e) {
                LOGGER.error("业务功能发生了异常！", e);
                return;
            } finally {
                // 释放锁
                try {
                    LOGGER.info("{} 业务功能处理完了，马上开始释放锁！", this.clientName);
                    if (lock.isAcquiredInThisProcess()) {
                        lock.release();
                    }
                    if (lock.isAcquiredInThisProcess()) {
                        lock.release();
                    }
                } catch (Exception e) {
                    LOGGER.info("释放锁发生了异常！", e);
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
