package com.zgy.test;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author ZGY
 * @date 2019/12/25 15:11
 * @description Test08App, Curator 高级特性 leader 选举
 */
public class Test08App {

    private static final Logger LOGGER = LoggerFactory.getLogger(Test08App.class);

    /**
     * 使用 LeaderLatch
     */
    @Test
    public void test() throws Exception {
        List<CuratorFramework> clientList = new ArrayList<>();
        List<LeaderLatch> leaderLatchList = new ArrayList<>();
        ExponentialBackoffRetry retry = new ExponentialBackoffRetry(1000, 3);

        for (int i = 0; i < 5; i++) {
            CuratorFramework client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", 5000, 5000, retry);
            clientList.add(client);

            final LeaderLatch latch = new LeaderLatch(client, "/francis/leader", "#client" + i);
            latch.addListener(new LeaderLatchListener() {
                @Override
                public void isLeader() {
                    LOGGER.info("I am Leader, id: [{}]", latch.getId());
                }

                @Override
                public void notLeader() {
                    LOGGER.info("I am not Leader, id: [{}]", latch.getId());
                }
            });
            leaderLatchList.add(latch);

            client.start();
            latch.start();
        }

        LOGGER.info("程序停止10秒开始");
        TimeUnit.SECONDS.sleep(10);
        LOGGER.info("程序停止10秒结束");

        LeaderLatch currentLatch = null;
        for (LeaderLatch latch : leaderLatchList) {
            if (latch.hasLeadership()) {
                currentLatch = latch;
                break;
            }
        }
        LOGGER.info("current leader is {}", currentLatch.getId());
        currentLatch.close();
        LOGGER.info("release the leader {}", currentLatch.getId());

        TimeUnit.SECONDS.sleep(5);

        for (LeaderLatch latch : leaderLatchList) {
            if (latch.hasLeadership()) {
                currentLatch = latch;
                break;
            }
        }
        LOGGER.info("current leader is {}", currentLatch.getId());
        currentLatch.close();
        LOGGER.info("release the leader {}", currentLatch.getId());

        TimeUnit.SECONDS.sleep(10);

        for (LeaderLatch latch : leaderLatchList) {
            if (null != latch.getState() && !latch.getState().equals(LeaderLatch.State.CLOSED)) {
                CloseableUtils.closeQuietly(latch);
            }
        }
        for (CuratorFramework client : clientList) {
            CloseableUtils.closeQuietly(client);
        }
    }
}
