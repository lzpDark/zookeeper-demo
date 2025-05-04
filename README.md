# Tasks Coordinator with Zookeeper

this service should be deployed in multiple nodes, 1 node as leader, others as followers.

Leader:
- assign tasks to followers
- assign failed/timeout tasks to current followers

Follower:
- execute assigned tasks
