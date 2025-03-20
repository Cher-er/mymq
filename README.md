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

   伪代码：

   ```java
   switch (action) {
       case "PUBLISH": {
           if (queue == null) {
               structureLock.lock();
               queues.put(queueName, new QueueHolder());
           }
           queue.writeLock().lock();
           queue.offer(message);
       }
       case "CONSUME": {
           queue.writeLock().lock();
           queue.poll();
       }
       case "CREATE": {
           structureLock.lock();
           queues.put(queueName, new QueueHolder());
       }
       case "DROP": {
           structureLock.lock();
           queue.writeLock().lock();
           queues.remove(queueName);
       }
   }
   ```

2. 从节点复制主节点消息的方式：

   主节点记录每个从节点连接的游标，读取日志文件，再推送给从节点。

   建立日志索引，快速根据游标定位日志内容。

3. 保证从节点消息顺序性的方式：

   将更新本地数据（内存、日志）和推送消息（到从节点）这两步都提交给线程池的话，无法保证从节点收到消息的顺序。

   由于内存和日志的数据具有一致性，可以专门启动一个线程，通过读取日志的方式将消息发送给从节点。

   伪代码：

   ```java
   private static void startReplicationThread() {
       new Thread(() -> {
           while (true) {
               replicaChannels.forEach((channel, cursor) -> {
                   String line = readLine(cursor++);
                   channel.writeAndFlush(line + '\n');
               });
           }
       }, "ReplicationThread").start();
   }
   ```

   
