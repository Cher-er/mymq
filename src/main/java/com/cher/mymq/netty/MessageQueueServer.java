package com.cher.mymq.netty;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MessageQueueServer {
    private static final String LOG_FILE = "messagequeue.log";

    // 封装队列和锁
    public static class QueueHolder {
        private final LinkedBlockingQueue<String> queue;
        private final ReentrantReadWriteLock lock;

        public QueueHolder() {
            this.queue = new LinkedBlockingQueue<>();
            this.lock = new ReentrantReadWriteLock();
        }

        public boolean offer(String e) {
            return queue.offer(e);
        }

        public String poll() {
            return queue.poll();
        }

        public ReentrantReadWriteLock.WriteLock writeLock() {
            return lock.writeLock();
        }
    }

    // 用于管理所有队列
    private ConcurrentHashMap<String, QueueHolder> queues = new ConcurrentHashMap<>();
    // 用于保护队列集合结构修改（CREATE/DROP）的全局锁
    private final ReentrantLock structureLock = new ReentrantLock();

    // 持久化日志工具
    private PersistentLogger logger;

    public MessageQueueServer() {
        try {
            logger = new PersistentLogger(LOG_FILE);
        } catch (IOException e) {
            System.err.println("[ERROR] 初始化持久化日志失败: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 解析并执行命令，返回操作响应
     *
     * 命令格式：
     *   PUBLISH queueName message
     *   CONSUME queueName
     *   CREATE queueName
     *   DROP queueName
     *
     * @param command 命令字符串
     * @param shouldLog 是否记录日志（恢复数据时为 false）
     * @return 操作结果响应
     */
    public String applyCommand(String command, boolean shouldLog) {
        String[] parts = command.split(" ", 3);
        if (parts.length < 2) {
            return "ERROR: 无效的命令格式";
        }
        String action = parts[0].toUpperCase();
        String queueName = parts[1];

        switch (action) {
            case "PUBLISH" -> {
                if (parts.length < 3) {
                    return "ERROR: PUBLISH 命令需要消息内容";
                }
                QueueHolder queue = queues.get(queueName);
                if (queue == null) {
                    // 自动创建队列
                    structureLock.lock();
                    try {
                        if (!queues.containsKey(queueName)) {
                            queues.put(queueName, new QueueHolder());
                            System.out.println("[INFO] 自动创建队列: " + queueName);
                        }
                        queue = queues.get(queueName);
                    } finally {
                        structureLock.unlock();
                    }
                }
                // 使用队列级别的写锁保证操作和日志写入原子执行
                queue.writeLock().lock();
                try {
                    String message = parts[2];
                    queue.offer(message);
                    if (shouldLog) {
                        logger.log(command);
                    }
                    System.out.println("[INFO] 消息已发布到队列 " + queueName + ": " + message);
                    return "OK: 消息已发布";
                } finally {
                    queue.writeLock().unlock();
                }
            }
            case "CONSUME" -> {
                QueueHolder queue = queues.get(queueName);
                if (queue == null) {
                    return "ERROR: 队列不存在";
                }
                queue.writeLock().lock();
                try {
                    String consumed = queue.poll();
                    if (consumed == null) {
                        return "NO_MESSAGE";
                    }
                    if (shouldLog) {
                        logger.log(command);
                    }
                    System.out.println("[INFO] 消息从队列 " + queueName + " 被消费: " + consumed);
                    return "MESSAGE: " + consumed;
                } finally {
                    queue.writeLock().unlock();
                }
            }
            case "CREATE" -> {
                structureLock.lock();
                try {
                    if (queues.containsKey(queueName)) {
                        return "ERROR: 队列已存在";
                    }
                    queues.put(queueName, new QueueHolder());
                    if (shouldLog) {
                        logger.log(command);
                    }
                    System.out.println("[INFO] 队列已创建: " + queueName);
                    return "OK: 队列已创建";
                } finally {
                    structureLock.unlock();
                }
            }
            case "DROP" -> {
                structureLock.lock();
                try {
                    QueueHolder queue = queues.get(queueName);
                    if (queue == null) {
                        return "ERROR: 队列不存在";
                    } else {
                        queue.writeLock().lock();
                        try {
                            queues.remove(queueName);
                            if (shouldLog) {
                                logger.log(command);
                            }
                            System.out.println("[INFO] 队列已删除: " + queueName);
                            return "OK: 队列已删除";
                        } finally {
                            queue.writeLock().unlock();
                        }
                    }
                } finally {
                    structureLock.unlock();
                }
            }
            default -> {
                return "ERROR: 未知命令";
            }
        }
    }

    /**
     * 加载持久化日志数据，重放命令以恢复内存队列状态
     */
    public void loadPersistedData() {
        System.out.println("[INFO] 正在加载持久化数据...");
        try {
            List<String> commands = PersistentLogger.readLog(LOG_FILE);
            for (String cmd : commands) {
                String resp = applyCommand(cmd, false);
                System.out.println("[INFO] 重放命令: " + cmd + " -> " + resp);
            }
            System.out.println("[INFO] 持久化数据加载完毕");
        } catch (IOException e) {
            System.out.println("[ERROR] 加载持久化数据失败: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 持久化日志工具，基于 Java NIO FileChannel 以追加方式写入日志文件
     */
    public static class PersistentLogger {
        private java.nio.channels.FileChannel channel;
        private Path logFilePath;

        public PersistentLogger(String filePath) throws IOException {
            this.logFilePath = Paths.get(filePath);
            channel = java.nio.channels.FileChannel.open(logFilePath,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        }

        public void log(String record) {
            String line = record + "\n";
            ByteBuffer buffer = ByteBuffer.wrap(line.getBytes(StandardCharsets.UTF_8));
            try {
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                channel.force(true);
            } catch (IOException e) {
                System.err.println("[ERROR] 写入日志失败：" + e.getMessage());
            }
        }

        public void close() {
            try {
                channel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public static List<String> readLog(String filePath) throws IOException {
            Path path = Paths.get(filePath);
            if (!Files.exists(path)) {
                return Collections.emptyList();
            }
            return Files.readAllLines(path, StandardCharsets.UTF_8);
        }
    }
}
