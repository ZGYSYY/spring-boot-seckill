package com.zgy.test;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author ZGY
 * @date 2019/12/26 14:04
 * @description Test10App
 */
public class Test10App {

    private static final Logger LOGGER = LoggerFactory.getLogger(Test10App.class);

    @Test
    public void test() throws InterruptedException {
        // 创建需要共享的资源对象
        final FakeLimitedResource resource = new FakeLimitedResource();
        // 创建线程池对象
        ExecutorService executorService = Executors.newFixedThreadPool(2);

        final ExponentialBackoffRetry retry = new ExponentialBackoffRetry(1000, 3);

        for (int i = 0; i < 2; i++) {
            final int index = i;
            Callable<Void> task = () -> {
                CuratorFramework client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", 5000, 5000, retry);
                try {
                    client.start();
                    InterProcessMutexDemo mutexDemo = new InterProcessMutexDemo(client, "/examples/locks", resource, "我是客户端: " + index);
                    for (int j = 0; j < 10; j++) {
                        mutexDemo.doWork(10, TimeUnit.SECONDS);
                    }
                } catch (Exception e) {
                    LOGGER.error("程序出现异常！", e);
                } finally {
                    // 关闭会话
                    CloseableUtils.closeQuietly(client);
                }
                return null;
            };

            // 交给线程池执行
            executorService.submit(task);
        }

        // 当线程池中的所有任务执行完后，关闭线程池
        executorService.shutdown();

        // 等待除主线程外其他线程都执行完毕
        executorService.awaitTermination(10, TimeUnit.MINUTES);
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

    /**
     * 锁
     */
    private class InterProcessMutexDemo {

        private InterProcessMutex lock;
        private FakeLimitedResource resource;
        private String clientName;

        public InterProcessMutexDemo(CuratorFramework client, String lockPath, FakeLimitedResource resource, String clientName) {
            this.lock = new InterProcessMutex(client, lockPath);
            this.resource = resource;
            this.clientName = clientName;
        }

        public void doWork(long time, TimeUnit unit) throws Exception {
            /*try {
                // 如果获取不到锁
                LOGGER.info("{}， 第一次加锁", this.clientName);
                if (!lock.acquire(time, unit)) {
                    throw new RuntimeException(this.clientName + "，第一次获取锁失败！");
                }

                // 第二次获取锁测试可重入性
                try {
                    LOGGER.info("{}， 第二次加锁", this.clientName);
                    if (!lock.acquire(time, unit)) {
                        throw new RuntimeException(this.clientName + "，第二次获取锁失败！");
                    }
                    LOGGER.info("{} 获取到了锁！", this.clientName);
                    resource.use();
                } finally {
                    LOGGER.info("{} 资源使用完毕，释第二次加的锁！", this.clientName);
                    lock.release();
                }

                LOGGER.info("{} 获取到了锁！", this.clientName);
                resource.use();
            } finally {
                LOGGER.info("{} 资源使用完毕，释第一次加的锁！", this.clientName);
                lock.release();
            }*/

            try {
                // 如果获取不到锁
                LOGGER.info("{}， 第一次加锁", this.clientName);
                if (!lock.acquire(time, unit)) {
                    throw new RuntimeException(this.clientName + "，第一次获取锁失败！");
                }

                LOGGER.info("{}， 第二次加锁", this.clientName);
                if (!lock.acquire(time, unit)) {
                    throw new RuntimeException(this.clientName + "，第二次获取锁失败！");
                }

                LOGGER.info("{} 获取到了锁！", this.clientName);
                resource.use();
            } finally {
                LOGGER.info("{} 资源使用完毕，释第一次加的锁！", this.clientName);
                lock.release();
                lock.release();
            }
        }
    }
}
