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

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
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
    public void test() throws IOException {
        List<CuratorFramework> clients = new ArrayList<>();
        List<LeaderSelectorAdapter> examples = new ArrayList<>();
        try {
            for (int i = 0; i < 10; i++) {
                CuratorFramework client
                        = CuratorFrameworkFactory.newClient("127.0.0.1:2181", new ExponentialBackoffRetry(20000, 3));
                clients.add(client);
                LeaderSelectorAdapter selectorAdapter = new LeaderSelectorAdapter(client, "/francis/leader", "Client #" + i);
                examples.add(selectorAdapter);
                client.start();
                selectorAdapter.start();
            }

            Scanner scanner = new Scanner(System.in);
            String next = scanner.next();
            if (next != null) {
                System.exit(-1);
            }
        } finally {
            LOGGER.info("Shutting down...");
            for (LeaderSelectorAdapter exampleClient : examples) {
                CloseableUtils.closeQuietly(exampleClient);
            }
            for (CuratorFramework client : clients) {
                CloseableUtils.closeQuietly(client);
            }
        }
    }

    private class LeaderSelectorAdapter extends LeaderSelectorListenerAdapter implements Closeable {

        private final String name;
        private final LeaderSelector leaderSelector;
        private final AtomicInteger leaderCount = new AtomicInteger();

        public LeaderSelectorAdapter(CuratorFramework client, String path, String name) {
            this.name = name;
            this.leaderSelector = new LeaderSelector(client, path, this);
            this.leaderSelector.autoRequeue();
        }

        public void start() throws IOException {
            leaderSelector.start();
        }

        @Override
        public void close() throws IOException {
            leaderSelector.close();
        }

        @Override
        public void takeLeadership(CuratorFramework client) throws Exception {
            final int waitSeconds = new Random().nextInt(5);
            LOGGER.info("{} is now the leader. Waiting {} seconds...", name, waitSeconds);
            LOGGER.info("{} has been leader {} time(s) before.", name, leaderCount.getAndIncrement());
            TimeUnit.SECONDS.sleep(waitSeconds);
        }
    }
}
