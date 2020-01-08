package com.zgy.test;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.queue.DistributedDelayQueue;
import org.apache.curator.framework.recipes.queue.QueueBuilder;
import org.apache.curator.framework.recipes.queue.QueueConsumer;
import org.apache.curator.framework.recipes.queue.QueueSerializer;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.TimeUnit;

/**
 * @author ZGY
 * @date 2020/1/8 15:53
 * @description Test20App, 分布式延迟队列—DistributedDelayQueue
 */
public class Test20App {

    private static final Logger LOGGER = LoggerFactory.getLogger(Test20App.class);

    @Test
    public void test() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", 10000, 5000, new ExponentialBackoffRetry(5000, 3));
        client.getCuratorListenable().addListener((client1, event) -> {
            LOGGER.info("监听客户端连接事件，事件名为：{}", event.getType().name());
        });
        client.start();

        // 创建延时队列对象
        DistributedDelayQueue<String> queue = QueueBuilder.builder(client, createQueueConsumer(), createQueueSerializer(), "/example/queue").buildDelayQueue();
        queue.start();

        for (int i = 0; i < 10; i++) {
            queue.put("Test-" + i, System.currentTimeMillis() + 1000);
        }

        LOGGER.info("所有的数据都已经放入了延时队列中！");

        // 等待消费者消费完队列数据再释放资源
        TimeUnit.SECONDS.sleep(10);

        // 释放资源
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
