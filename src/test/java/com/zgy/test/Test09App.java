package com.zgy.test;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author ZGY
 * @date 2019/12/25 17:10
 * @description Test09App, Curator 高级特性 leader 选举
 */
public class Test09App {

    private static final Logger LOGGER = LoggerFactory.getLogger(Test09App.class);

    @Test
    public void test() throws Exception {
        List<CuratorFramework> clients = new ArrayList<>();
        List<LeaderSelectorAdapter> examples = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            CuratorFramework client = CuratorFrameworkFactory.newClient("127.0.0.1:2181",
                    new ExponentialBackoffRetry(20000, 3));
            clients.add(client);
            LeaderSelectorAdapter selectorAdapter = new LeaderSelectorAdapter(client, "/francis/leader", "Client #" + i);
            examples.add(selectorAdapter);
            // 连接 zookeeper 服务器
            client.start();
            // 开始执行 leader 选举
            selectorAdapter.start();
        }

        // 测试完毕后，关闭选举和会话
        TimeUnit.SECONDS.sleep(30);
        LOGGER.info("开始回收数据了哦！");
        for (LeaderSelectorAdapter example : examples) {
            CloseableUtils.closeQuietly(example);
        }
        for (CuratorFramework client : clients) {
            CloseableUtils.closeQuietly(client);
        }
    }

    private class LeaderSelectorAdapter extends LeaderSelectorListenerAdapter implements Closeable {

        private final String name;
        private final LeaderSelector leaderSelector;
        private final AtomicInteger leaderCount = new AtomicInteger();

        public LeaderSelectorAdapter(CuratorFramework client, String path, String name) {
            this.name = name;
            this.leaderSelector = new LeaderSelector(client, path, this);
            // 希望一个 selector 放弃 leader 后还要重新参与leader选举
            this.leaderSelector.autoRequeue();
        }

        public void start() throws IOException {
            leaderSelector.start();
        }

        @Override
        public void close() throws IOException {
            leaderSelector.close();
        }

        /**
         * 当某个实例成为 leader 后就会执行这个方法，当这个方法执行完后，该实例就会放弃 leader 的执行权。
         * @param client
         * @throws Exception
         */
        @Override
        public void takeLeadership(CuratorFramework client) throws Exception {
            final int waitSeconds = new Random().nextInt(5);
            LOGGER.info("{} 现在是 leader，接下来我会一直当 leader {} 秒钟，除开这一次，我已经当过 {} 次 leader 了！", name, waitSeconds, leaderCount.getAndIncrement());
            TimeUnit.SECONDS.sleep(waitSeconds);
        }
    }
}
