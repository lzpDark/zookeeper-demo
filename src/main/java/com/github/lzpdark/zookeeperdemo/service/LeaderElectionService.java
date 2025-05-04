package com.github.lzpdark.zookeeperdemo.service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

/**
 * @author lzp
 */
@Service
public class LeaderElectionService {

    private static final Logger logger = LoggerFactory.getLogger(LeaderElectionService.class);
    private static final String ELECTION_PATH = "/election/leader";

    private final CuratorFramework client;
    private LeaderLatch leaderLatch;
    private volatile boolean isLeader = false;

    @Autowired
    public LeaderElectionService(CuratorFramework client) {
        this.client = client;
    }

    @PostConstruct
    public void init() throws Exception {
        // 创建选举路径(如果不存在)
        try {
            if (client.checkExists().forPath(ELECTION_PATH) == null) {
                client.create()
                        .creatingParentsIfNeeded()
                        .forPath(ELECTION_PATH);
            }
        } catch (Exception e) {
            logger.error("Error creating election path", e);
        }

        client.getConnectionStateListenable().addListener(new ConnectionStateListener() {
            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                logger.info("zookeeper connection state changed to: {}", newState);
            }
        });

        // 初始化选举实例
        leaderLatch = new LeaderLatch(client, ELECTION_PATH, getNodeId());

        // 添加监听器
        leaderLatch.addListener(new LeaderLatchListener() {
            @Override
            public void isLeader() {
                isLeader = true;
                logger.info("Node {} is now the leader, executing leader logic", getNodeId());
            }

            @Override
            public void notLeader() {
                isLeader = false;
                logger.info("Node {} is no longer the leader, executing follower logic", getNodeId());
            }
        });
        // 启动选举
        leaderLatch.start();
    }

    @PreDestroy
    public void destroy() throws Exception {
        if (leaderLatch != null) {
            leaderLatch.close();
        }
    }

    public boolean isLeader() {
        return isLeader;
    }

    // 获取唯一的节点ID
    private String getNodeId() {
        try {
            return InetAddress.getLocalHost().getHostName() + "-" + UUID.randomUUID();
        } catch (UnknownHostException e) {
            return "node-" + UUID.randomUUID();
        }
    }
}
