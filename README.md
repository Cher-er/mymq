# MyMQ

MQ Server 单机应用，通过追加日志实现持久化

- bio：基于BIO实现的 MQ Server，以及与 Server 进行连接的 MQ Client
- nio：基于NIO实现的 MQ Server
- netty：基于Netty实现的 MQ Server

MQ Server 分布式应用，实现简单的主从部署

- distributed.master：MQ Server 主节点
- distributed.slave：MQ Server 从节点



1. 保证内存和日志数据一致性的方式：

   Publish、Consume 操作需要获得对应的队列锁。

   Create 操作需要获得结构锁。

   Drop 操作需要先获得结构锁、再获得队列锁。

2. 从节点复制主节点消息的方式：

   主节点记录每个从节点连接的游标，读取日志文件，再推送给从节点。

   建立日志索引，快速根据游标定位日志内容。
