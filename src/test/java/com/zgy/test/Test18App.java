package com.zgy.test;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.queue.*;
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
 * @date 2020/1/8 13:55
 * @description Test18App，带Id的分布式队列—DistributedIdQueue
 */
public class Test18App {

    private static final Logger LOGGER = LoggerFactory.getLogger(Test18App.class);

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

        // 创建队列对象
        DistributedIdQueue<String> queue = QueueBuilder.builder(client, createQueueConsumer(), createQueueSerializer(), "/example/queue").buildIdQueue();
        // 启动队列
        queue.start();

        Random random = new Random();
        for (int i = 0; i < 10; i++) {
            queue.put("Test-" + i, "Id-" + i);
            /**
             * 往队列中放入一个数据后睡眠任意秒，该线程放弃 CPU 执行权。队列开始消费，如果该线程在睡眠任意秒后重新获得 CPU 执行权，但是队列的数据没有被消费，
             * 此时调用 queue.remove("Id-" + i) 方法把队列中的指定数据删除了，该消费就不会被消费了。
             */
            TimeUnit.SECONDS.sleep(random.nextInt(2));
            queue.remove("Id-" + i);
        }

        TimeUnit.SECONDS.sleep(2);

        LOGGER.info("程序执行完毕！准备回收资源");

        CloseableUtils.closeQuietly(queue);
        CloseableUtils.closeQuietly(client);

        LOGGER.info("资源回收完毕！");
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
