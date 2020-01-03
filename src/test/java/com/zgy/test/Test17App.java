package com.zgy.test;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.queue.DistributedQueue;
import org.apache.curator.framework.recipes.queue.QueueBuilder;
import org.apache.curator.framework.recipes.queue.QueueConsumer;
import org.apache.curator.framework.recipes.queue.QueueSerializer;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.TimeUnit;

/**
 * @author ZGY
 * @date 2020/1/3 14:43
 * @description Test17App, 分布式队列—DistributedQueue
 */
public class Test17App {

    private static final Logger LOGGER = LoggerFactory.getLogger(Test17App.class);

    @Test
    public void test() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", 10000, 5000, new ExponentialBackoffRetry(1000, 3));
        CuratorFramework client2 = CuratorFrameworkFactory.newClient("127.0.0.1:2181", 10000, 5000, new ExponentialBackoffRetry(1000, 3));

        // 客户端启动
        client.start();
        // 客户端2启动
        client2.start();

        /**
         * 是线程不安全的
         */
        // DistributedQueue<String> queue = QueueBuilder.builder(client, createQueueConsumer("消费者A"), creQueueSerializer(), "/example/queue").buildQueue();
        // DistributedQueue<String> queue2 = QueueBuilder.builder(client, createQueueConsumer("消费者B"), creQueueSerializer(), "/example/queue").buildQueue();

        /**
         * 正常情况下先将消息从队列中移除，再交给消费者消费。但这是两个步骤，不是原子的。
         * 可以调用Builder的lockPath()消费者加锁，当消费者消费数据时持有锁，这样其它消费者不能消费此消息。
         * 如果消费失败或者进程死掉，消息可以交给其它进程。这会带来一点性能的损失。最好还是单消费者模式使用队列。
         */
        DistributedQueue<String> queue = QueueBuilder.builder(client, createQueueConsumer("消费者A"), creQueueSerializer(), "/example/queue").lockPath("/example/lock").buildQueue();
        DistributedQueue<String> queue2 = QueueBuilder.builder(client, createQueueConsumer("消费者B"), creQueueSerializer(), "/example/queue").lockPath("/example/lock").buildQueue();

        // 消息队列启动
        queue.start();
        // 消息队列2启动
        queue2.start();

        for (int i = 0; i < 100; i++) {
            // 往消息队列中放数据
            queue.put("Test-A-" + i);
            TimeUnit.MILLISECONDS.sleep(10);
            // 往消息队列2中放数据
             queue2.put("Test-B-" + i);
        }

        TimeUnit.SECONDS.sleep(20);

        // 关闭消息队列
        queue.close();
        // 关闭消息队列2
        queue2.close();

        // 客户端关闭
        client.close();
        // 客户端2关闭
        client2.close();
        LOGGER.info("程序执行结束！");
    }

    /**
     * 定义队列消费者
     * @return
     */
    private QueueConsumer<String> createQueueConsumer(final String name) {
        return new QueueConsumer<String>() {
            /**
             * 消费消息时调用的方法
             * @param message
             * @throws Exception
             */
            @Override
            public void consumeMessage(String message) throws Exception {
                LOGGER.info("{} 消费消息：{}", name, message);
            }

            /**
             * 状态改变时调用的方法
             * @param client
             * @param newState
             */
            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                LOGGER.info("连接状态为：{}", newState.name());
            }
        };
    }

    /**
     * 队列消息序列化实现类
     * @return
     */
    private QueueSerializer<String> creQueueSerializer() {
        return new QueueSerializer<String>() {
            /**
             * 序列化
             * @param item
             * @return
             */
            @Override
            public byte[] serialize(String item) {
                return item.getBytes();
            }

            /**
             * 反序列化
             * @param bytes
             * @return
             */
            @Override
            public String deserialize(byte[] bytes) {
                return new String(bytes);
            }
        };
    }
}
