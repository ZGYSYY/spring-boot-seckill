package com.zgy.test;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * @author ZGY
 * @date 2019/12/25 11:19
 * @description Test06App, Curator 高级特性 Node Cache
 */
public class Test06App {

    private static final Logger LOGGER = LoggerFactory.getLogger(Test06App.class);

    @Test
    public void test() throws Exception {
        // 创建客户端 CuratorFramework 对象
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectionTimeoutMs(5000)
                .connectString("127.0.0.1:2181")
                .sessionTimeoutMs(5000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();

        // 连接 zookeeper服务器
        client.start();

        // 创建一个 NodeCache 对象来监听指定节点
        NodeCache nodeCache = new NodeCache(client, "/example/cache");
        // 当节点数据变化时需要处理的逻辑
        nodeCache.getListenable().addListener(() -> {
            ChildData currentData = nodeCache.getCurrentData();
            if (null != currentData) {
                LOGGER.info("节点数据：Path[{}], Data: [{}]",currentData.getPath() , new String(currentData.getData()));
            } else {
                LOGGER.info("节点被删除！");
            }
        });
        // 开始监听子节点变化
        nodeCache.start();

        // 创建节点
        client.create().creatingParentsIfNeeded().forPath("/example/cache", "test01".getBytes());
        TimeUnit.MILLISECONDS.sleep(100);

        // 修改数据
        client.setData().forPath("/example/cache", "test01_V1".getBytes());
        TimeUnit.MILLISECONDS.sleep(100);

        // 获取节点数据
        String s = new String(client.getData().forPath("/example/cache"));
        LOGGER.info("数据s：[{}]", s);

        // 删除节点
        client.delete().forPath("/example/cache");
        TimeUnit.MILLISECONDS.sleep(100);

        // 删除测试用的数据，如果存在子节点，一并删除
        client.delete().deletingChildrenIfNeeded().forPath("/example");

        // 关闭监听
        nodeCache.close();

        // 断开与 zookeeper 的连接
        client.close();

        LOGGER.info("程序执行完毕！");

        // 为了查看打印日志，不加这段代码看不到节点监听处理逻辑
        for (;;);
    }
}
