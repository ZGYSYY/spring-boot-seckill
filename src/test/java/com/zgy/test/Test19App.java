package com.zgy.test;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.queue.DistributedPriorityQueue;
import org.apache.curator.framework.recipes.queue.QueueBuilder;
import org.apache.curator.framework.recipes.queue.QueueConsumer;
import org.apache.curator.framework.recipes.queue.QueueSerializer;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author ZGY
 * @date 2020/1/8 15:32
 * @description Test19App, 优先级分布式队列—DistributedPriorityQueue
 */
public class Test19App {

    private static final Logger LOGGER = LoggerFactory.getLogger(Test19App.class);

    /**
     * 测试方法
     */
    @Test
    public void test() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", 10000, 5000, new ExponentialBackoffRetry(5000, 3));
        client.getCuratorListenable().addListener((client1, event) -> {
            LOGGER.info("监听客户端连接事件，事件名为：{}", event.getType().name());
        });
        client.start();

        // 创建优先级队列对象
        DistributedPriorityQueue<String> queue = QueueBuilder.builder(client, createQueueConsumer(), createQueueSerializer(), "/example/queue").buildPriorityQueue(0);
        queue.start();

        Random random = new Random();
        for (int i = 0; i < 10; i++) {
            int priority = random.nextInt(100);
            LOGGER.info("Test-" + i + " priority: " + priority);
            queue.put("Test" + i, priority);
            TimeUnit.SECONDS.sleep(random.nextInt(2));
        }

        LOGGER.info("程序执行完毕！开始回收资源");

        CloseableUtils.closeQuietly(queue);
        CloseableUtils.closeQuietly(client);
    }

    /**
     * 创建序列化和反序列化对象
     * @return
     */
    private QueueSerializer<String> createQueueSerializer() {
        return new QueueSerializer<String>() {
            @Override
            public byte[] serialize(String item) {
                return item.getBytes();
            }

            @Override
            public String deserialize(byte[] bytes) {
                return new String(bytes);
            }
        };
    }

    /**
     * 创建消费者对象
     * @return
     */
    private QueueConsumer<String> createQueueConsumer() {
        return new QueueConsumer<String>() {
            @Override
            public void consumeMessage(String message) throws Exception {
                LOGGER.info("消费的消息内容为：{}", message);
            }

            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                LOGGER.info("当前连接的状态改变了，新状态为：{}", newState);
            }
        };
    }
}
