package com.zgy.test;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * @author ZGY
 * @date 2019/12/24 11:16
 * @description Zookeeper 客户端使用 Curator 案例
 */
public class Test04App {

    private static final Logger LOGGER = LoggerFactory.getLogger(Test04App.class);

    @Before
    public void before() throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.builder().connectString("127.0.0.1:2181")
                .sessionTimeoutMs(5000)
                .connectionTimeoutMs(5000)
                .retryPolicy(retryPolicy)
                .build();
        client.start();
        // 删除一个节点，并且递归删除其所有的子节点
        Stat stat = client.checkExists().forPath("/study");
        if (stat != null) {
            client.delete().deletingChildrenIfNeeded().forPath("/study");
        }
        LOGGER.info("清除上一次测试的数据成功！");
    }

    /**
     * 创建客户端连接（方式一）
     */
    @Test
    public void test() {
        /*
        创建重试策略对象，参数含义如下：
        - baseSleepTimeMs: 基本睡眠时间。
        - maxRetries：最大重试次数。
         */
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        /*
        创建客户端对象，参数含义如下：
        - connectString：服务器列表，格式为 `host1:port,host2:port,...`。
        - sessionTimeoutMs：会话超时时间， 默认 60000 ms。
        - connectionTimeoutMs：连接超时时间，默认 60000 ms。
        - retryPolicy：重试策略
         */
        CuratorFramework client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", 5000, 5000, retryPolicy);
        // 连接 zookeeper 服务器
        client.start();
    }

    /**
     * 创建客户端连接（方式二）
     */
    @Test
    public void test2() {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.builder().connectString("127.0.0.1:2181")
                .sessionTimeoutMs(5000)
                .connectionTimeoutMs(5000)
                .retryPolicy(retryPolicy)
                .build();
        client.start();
    }

    /**
     * 基本操作
     * @throws Exception
     */
    @Test
    public void test3() throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.builder().connectString("127.0.0.1:2181")
                .sessionTimeoutMs(5000)
                .connectionTimeoutMs(5000)
                .retryPolicy(retryPolicy)
                .namespace("study")
                .build();
        client.start();
        /**
         * 创建节点
         */
        // 创建一个节点，初始内容为空
        client.create().forPath("/name");
        // 创建一个节点，附带初始化内容
        client.create().forPath("/name2", "创建一个节点，附带初始化内容".getBytes(Charset.forName("utf-8")));
        // 创建一个节点，指定创建模式（临时节点），内容为空
        client.create().withMode(CreateMode.EPHEMERAL).forPath("/name3");
        // 创建一个节点，指定创建模式（临时节点），附带初始化内容
        client.create().withMode(CreateMode.EPHEMERAL).forPath("/name4", "创建一个节点，指定创建模式（临时节点），附带初始化内容".getBytes(Charset.forName("utf-8")));
        // 创建一个节点，指定创建模式（临时节点），附带初始化内容，并且自动递归创建父节点
        client.create().creatingParentContainersIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/parent/name5", "创建一个节点，指定创建模式（临时节点），附带初始化内容，并且自动递归创建父节点".getBytes(Charset.forName("utf-8")));

        /**
         * 更新数据节点数据
         */
        // 更新一个节点的数据内容
        client.setData().forPath("/name2", "更新一个节点的数据内容".getBytes(Charset.forName("utf-8")));
        // 更新一个节点的数据内容，强制指定版本进行更新
        client.setData().withVersion(0).forPath("/name", "更新一个节点的数据内容，强制指定版本进行更新".getBytes(Charset.forName("utf-8")));

        /**
         * 读取节点
         */
        // 读取一个节点的数据内容
        String s = new String(client.getData().forPath("/name2"), Charset.forName("utf-8"));
        LOGGER.info("读取一个节点的数据内容, s: [{}]", s);
        // 读取一个节点的数据内容，同时获取到该节点的stat
        Stat stat = new Stat();
        s = new String(client.getData().storingStatIn(stat).forPath("/name"), Charset.forName("utf-8"));
        LOGGER.info("读取一个节点的数据内容，同时获取到该节点的stat, s: [{}], stat: [{}]", s, stat);

        /**
         * 删除节点
         */
        // 删除一个节点
        client.delete().forPath("/name");
        // 删除一个节点，并且递归删除其所有的子节点
        client.delete().deletingChildrenIfNeeded().forPath("/parent");

        /**
         * 检查节点是否存在，不存在时对象为 null
         */
        stat = client.checkExists().forPath("/name5");
        LOGGER.info("检查节点是否存在, stat: [{}]", stat);
    }

    /**
     * 事务原子性操作
     * @throws Exception
     */
    @Test
    public void test4() throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString("127.0.0.1:2181")
                .sessionTimeoutMs(5000)
                .connectionTimeoutMs(5000)
                .retryPolicy(retryPolicy)
                .namespace("study")
                .build();
        client.start();

        // 事务操作，保证原子性
        client.inTransaction()
                .create().withMode(CreateMode.EPHEMERAL).forPath("/name", "aaaaa".getBytes())
                .and().setData().forPath("/name", "bbb".getBytes())
                .and().commit();
    }

    /**
     * 异步操作
     * @throws Exception
     */
    @Test
    public void test5() throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString("127.0.0.1:2181")
                .sessionTimeoutMs(5000)
                .connectionTimeoutMs(5000)
                .retryPolicy(retryPolicy)
                .namespace("study")
                .build();
        client.start();

        Executor executor = Executors.newFixedThreadPool(2);
        client.create().creatingParentContainersIfNeeded().withMode(CreateMode.EPHEMERAL).inBackground(new BackgroundCallback() {
            @Override
            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                LOGGER.info("开始调用回调方法，WatchedEvent: [{}], ResultCode: [{}]", event.getType(), event.getResultCode());
            }
        }, executor).forPath("/name");

        // 不让程序结束，否则看不到回调方法的调用
        for (;;);
    }
}
