package com.github.lzpdark.zookeeperdemo.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Set;

/**
 * @author lzp
 */
@Service
public class TaskService {

    private static final Logger logger = LoggerFactory.getLogger(TaskService.class);

    private final LeaderElectionService electionService;

    @Autowired
    public TaskService(LeaderElectionService electionService) {
        this.electionService = electionService;
    }

    @Scheduled(fixedRate = 30_000)
    public void assignTasks() {
        if (!electionService.isLeader()) {
            return;
        }
        // TODO: assign tasks to followers
        //  1. get n tasks
        //  2. assign to followers
        Set<String> followerNames = electionService.getFollowerNames();
    }

    @Scheduled(fixedRate = 30_000)
    public void assignFailedTasks() {
        if(!electionService.isLeader()) {
            return;
        }
        // TODO: get failed tasks, re-assign to followers or remove from tasks
    }

    @Scheduled(fixedRate = 30_000)
    public void assignTimeoutTasks() {
        // TODO: get timeout task
        Set<String> followerNames = electionService.getFollowerNames();
        // TODO: if not assigned to followers, reassign
    }

    // 根据选举状态执行相应逻辑的示例方法
    @Scheduled(fixedRate = 10_000)
    public void executeTask() {
        if(electionService.isLeader()) {
            return;
        }
        logger.info("execute task at fixed rate at {}.", System.currentTimeMillis());
        // TODO: get n assigned tasks
        //  idempotent check
        //  execute them
        //  update task status
    }
}
