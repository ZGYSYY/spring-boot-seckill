package com.zgy.test;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.queue.SimpleDistributedQueue;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * @author ZGY
 * @date 2020/1/8 16:22
 * @description Test21App，SimpleDistributedQueue
 */
public class Test21App {

    private static final Logger LOGGER = LoggerFactory.getLogger(Test21App.class);

    @Test
    public void test() throws InterruptedException {
        CuratorFramework client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", 10000, 5000, new ExponentialBackoffRetry(5000, 3));
        client.getCuratorListenable().addListener((client1, event) -> {
            LOGGER.info("监听客户端连接事件，事件名为：{}", event.getType().name());
        });
        client.start();

        // 创建队列对象
        SimpleDistributedQueue queue = new SimpleDistributedQueue(client, "/example/queue");

        // 创建生产者和消费者对象
        Producer producer = new Producer(queue);
        Consumer consumer = new Consumer(queue);

        // 启动生产者和消费者线程
        new Thread(producer).start();
        new Thread(consumer).start();

        // 等待队列中的数据处理完毕
        TimeUnit.SECONDS.sleep(10);

        // 释放资源
        CloseableUtils.closeQuietly(client);

        LOGGER.info("程序执行完毕！");
    }

    /**
     * 消费者
     */
    private class Consumer implements Runnable {

        private SimpleDistributedQueue queue;

        public Consumer(SimpleDistributedQueue queue) {
            this.queue = queue;
        }

        @Override
        public void run() {
            try {
                byte[] bytes = this.queue.take();
                LOGGER.info("消费一条消息成功：{}", new String(bytes));
            } catch (Exception e) {
                LOGGER.error("程序出现异常!", e);
                return;
            }
        }
    }

    private class Producer implements Runnable {

        private SimpleDistributedQueue queue;

        public Producer(SimpleDistributedQueue queue) {
            this.queue = queue;
        }

        @Override
        public void run() {
            for (int i = 0; i < 100; i++) {
                try {
                    boolean flag = this.queue.offer(("test-" + i).getBytes());
                    if (flag) {
                        LOGGER.info("发送消息成功：{}", "test-" + i);
                    } else {
                        LOGGER.info("发送消息失败：{}", "test-" + i);
                    }
                } catch (Exception e) {
                    LOGGER.error("程序发生异常！", e);
                    continue;
                }
            }
        }
    }
}
