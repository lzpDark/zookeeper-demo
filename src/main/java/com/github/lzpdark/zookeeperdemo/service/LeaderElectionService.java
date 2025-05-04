package com.github.lzpdark.zookeeperdemo.service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * @author lzp
 */
@Service
public class LeaderElectionService {

    private static final Logger logger = LoggerFactory.getLogger(LeaderElectionService.class);
    private static final String ELECTION_PATH = "/election/leader";
    private static final String FOLLOWERS_PATH = "/cluster/followers";

    private String nodeId;
    private CuratorCache followersCache;

    private final CuratorFramework client;
    private LeaderLatch leaderLatch;
    private volatile boolean isLeader = false;

    @Autowired
    public LeaderElectionService(CuratorFramework client) {
        this.client = client;
    }

    @PostConstruct
    public void init() throws Exception {
        handlerConnection();

        nodeId = generateNodeId();
        registerNode();

        electLeader();
        watchFollowers();
    }

    private void handlerConnection() {
        client.getConnectionStateListenable().addListener(new ConnectionStateListener() {
            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                logger.info("zookeeper connection state changed to: {}", newState);
            }
        });
    }

    // 获取唯一的节点ID
    private String generateNodeId() {
        try {
            return InetAddress.getLocalHost().getHostName() + "-" + UUID.randomUUID();
        } catch (UnknownHostException e) {
            return "node-" + UUID.randomUUID();
        }
    }

    // 注册当前节点到集群
    private void registerNode() throws Exception {
        try {
            // 创建临时顺序节点，节点消失时（如服务宕机），ZooKeeper会自动删除
            String nodePath = client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(FOLLOWERS_PATH + "/" + nodeId, "".getBytes(StandardCharsets.UTF_8));

            String registeredNodeId = extractNodeIdFromPath(nodePath);
            logger.info("Node registered with path: {}, extracted nodeId: {}", nodePath, registeredNodeId);

            // 更新本地nodeId为ZooKeeper分配的顺序ID
            this.nodeId = registeredNodeId;
        } catch (Exception e) {
            logger.error("Failed to register node", e);
            throw e;
        }
    }

    private void electLeader() throws Exception {
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
        // 初始化选举实例
        leaderLatch = new LeaderLatch(client, ELECTION_PATH, nodeId);

        // 添加监听器
        leaderLatch.addListener(new LeaderLatchListener() {
            @Override
            public void isLeader() {
                isLeader = true;
                logger.info("Node {} is now the leader, executing leader logic", nodeId);
            }

            @Override
            public void notLeader() {
                isLeader = false;
                logger.info("Node {} is no longer the leader, executing follower logic", nodeId);
            }
        });
        // 启动选举
        leaderLatch.start();
    }

    // 监控集群成员变化
    private void watchFollowers() {
        try {
            followersCache = CuratorCache.builder(client, FOLLOWERS_PATH).build();
            followersCache.listenable().addListener((type, oldData, data) -> {
                logger.info("follower changed, type:{}, oldPath:{}, path:{}",
                        type, oldData == null ? "null" : oldData.getPath(), data == null ? "null" : data.getPath());
            });
            followersCache.start();
            logger.info("Initial cluster members: {}", getFollowerNames());
        } catch (Exception e) {
            logger.error("Failed to set up cluster members watch", e);
            throw e;
        }
    }

    // 从ZK路径提取节点ID
    private String extractNodeIdFromPath(String path) {
        return path.substring(path.lastIndexOf('/') + 1);
    }

    @PreDestroy
    public void destroy() throws Exception {
        if (leaderLatch != null) {
            leaderLatch.close();
        }
        if(followersCache != null) {
            followersCache.close();
        }
    }

    public boolean isLeader() {
        return isLeader;
    }

    public Set<String> getFollowerNames() {
        List<ChildData> currentData = followersCache.stream().toList();
        Set<String> followerNames = new HashSet<>();

        for (ChildData data : currentData) {
            if(data.getPath().equals(FOLLOWERS_PATH)) {
                continue;
            }
            String nodeIdFromPath = extractNodeIdFromPath(data.getPath());
            followerNames.add(nodeIdFromPath);
        }
        return followerNames;
    }
}
