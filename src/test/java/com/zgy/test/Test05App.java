package com.zgy.test;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * @author ZGY
 * @date 2019/12/25 10:31
 * @description Test05App, Curator 高级特性案例
 */
public class Test05App {

    private static final Logger LOGGER = LoggerFactory.getLogger(Test05App.class);

    @Test
    public void test() throws Exception {
        // 创建客户端 CuratorFramework 对象
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .connectString("127.0.0.1:2181")
                .sessionTimeoutMs(5000)
                .connectionTimeoutMs(5000)
                .build();
        // 连接 zookeeper服务器
        client.start();
        // 创建一个 PathChildrenCache 对象来监听对应路径下的的子节点
        PathChildrenCache pathChildrenCache = new PathChildrenCache (client, "/example/cache", true);
        // 开始监听子节点变化
        pathChildrenCache.start();
        // 当子节点数据变化时需要处理的逻辑
        pathChildrenCache.getListenable().addListener((clientFramework, event) -> {
            LOGGER.info("事件类型为：{}", event.getType());
            if (null != event.getData()) {
                LOGGER.info("节点路径为：{}，节点数据为：{}", event.getData().getPath(), new String(event.getData().getData()));
            }
        });

        // 创建节点
        client.create().creatingParentsIfNeeded().forPath("/example/cache/test01", "01".getBytes());
        TimeUnit.MILLISECONDS.sleep(10);

        // 创建节点
        client.create().creatingParentsIfNeeded().forPath("/example/cache/test02", "02".getBytes());
        TimeUnit.MILLISECONDS.sleep(10);

        // 修改数据
        client.setData().forPath("/example/cache/test01", "01_V2".getBytes());
        TimeUnit.MILLISECONDS.sleep(10);

        // 遍历缓存中的数据
        for (ChildData childData : pathChildrenCache.getCurrentData()) {
            LOGGER.info("获取childData对象数据, Path: [{}], Data: [{}]", childData.getPath(), new String(childData.getData()));
        }

        // 删除数据
        client.delete().forPath("/example/cache/test01");
        TimeUnit.MILLISECONDS.sleep(10);

        // 删除数据
        client.delete().forPath("/example/cache/test02");
        TimeUnit.MILLISECONDS.sleep(10);

        // 关闭监听
        pathChildrenCache.close();

        // 删除测试用的数据，如果存在子节点，一并删除
        client.delete().deletingChildrenIfNeeded().forPath("/example");

        // 断开与 zookeeper 的连接
        client.close();
        LOGGER.info("程序执行完毕");
    }
}
