package com.zgy.test;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.TimeUnit;

/**
 * @author ZGY
 * @date 2019/12/25 11:45
 * @description Test07App, Curator 高级特性 Tree Cache
 */
public class Test07App {

    private static final Logger LOGGER = LoggerFactory.getLogger(Test07App.class);

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

        // 创建一个 NodeCache 对象来监听指定节点下的所有节点
        TreeCache treeCache = new TreeCache(client, "/example/cache");
        // 当指定节点下的某个节点数据变化时需要处理的逻辑
        treeCache.getListenable().addListener((curatorFramework, event) -> {
            LOGGER.info("事件类型：{}， 路径：{}，数据：{}",
                    event.getType(),
                    event.getData() == null? null:event.getData().getPath(),
                    event.getData() == null? null:new String(event.getData().getData()));
        });

        // 开始监听指定节点下的所有节点变化
        treeCache.start();

        // 创建节点
        client.create().creatingParentsIfNeeded().forPath("/example/cache", "test01".getBytes());
        TimeUnit.MILLISECONDS.sleep(100);

        // 修改数据
        client.setData().forPath("/example/cache", "test01_V2".getBytes());
        TimeUnit.MILLISECONDS.sleep(100);

        // 修改数据
        client.setData().forPath("/example/cache", "test01_V3".getBytes());
        TimeUnit.MILLISECONDS.sleep(100);

        // 删除节点
        client.delete().forPath("/example/cache");
        TimeUnit.MILLISECONDS.sleep(100);

        // 删除测试用的数据，如果存在子节点，一并删除
        client.delete().deletingChildrenIfNeeded().forPath("/example");

        // 关闭监听
        treeCache.close();

        // 断开与 zookeeper 的连接
        client.close();

        LOGGER.info("程序执行完毕！");

        // 为了查看打印日志，不加这段代码看不到节点监听处理逻辑
        for (;;);
    }
}
