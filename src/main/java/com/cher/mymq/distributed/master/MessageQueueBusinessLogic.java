package com.cher.mymq.distributed.master;

import com.cher.mymq.nio.MessageQueueServer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MessageQueueBusinessLogic {
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

    // 持久化日志工具（可选，本示例略简化）
    private PersistentLogger logger;

    public MessageQueueBusinessLogic(String logFilePath) {
        try {
            logger = new PersistentLogger(logFilePath);
            loadPersistedData(logFilePath);
        } catch (IOException e) {
            System.err.println("[ERROR] 初始化日志失败: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void loadPersistedData(String logFilePath) {
        System.out.println("[INFO] 正在加载持久化数据...");
        try {
            List<String> commands = PersistentLogger.readLog(logFilePath);
            for (String cmd : commands) {
                // 重放命令时不再记录到日志，避免重复写入
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
            return "ERROR: 无效命令格式";
        }
        String action = parts[0].toUpperCase();
        String queueName = parts[1];

        switch (action) {
            case "PUBLISH": {
                if (parts.length < 3) {
                    return "ERROR: PUBLISH 命令需要消息";
                }
                QueueHolder queue = queues.get(queueName);
                if (queue == null) {
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
                queue.writeLock().lock();
                try {
                    String message = parts[2];
                    queue.offer(message);
                    if (shouldLog && logger != null) {
                        logger.log(command);
                    }
                    System.out.println("[INFO] 消息已发布到队列 " + queueName + ": " + message);
                    return "OK: 消息已发布";
                } finally {
                    queue.writeLock().unlock();
                }
            }
            case "CONSUME": {
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
                    if (shouldLog && logger != null) {
                        logger.log(command);
                    }
                    System.out.println("[INFO] 消息从队列 " + queueName + " 被消费: " + consumed);
                    return "MESSAGE: " + consumed;
                } finally {
                    queue.writeLock().unlock();
                }
            }
            case "CREATE": {
                structureLock.lock();
                try {
                    if (queues.containsKey(queueName)) {
                        return "ERROR: 队列已存在";
                    }
                    queues.put(queueName, new QueueHolder());
                    if (shouldLog && logger != null) {
                        logger.log(command);
                    }
                    System.out.println("[INFO] 队列已创建: " + queueName);
                    return "OK: 队列已创建";
                } finally {
                    structureLock.unlock();
                }
            }
            case "DROP": {
                structureLock.lock();
                try {
                    QueueHolder queue = queues.get(queueName);
                    if (queue == null) {
                        return "ERROR: 队列不存在";
                    }
                    queue.writeLock().lock();
                    try {
                        queues.remove(queueName);
                        if (shouldLog && logger != null) {
                            logger.log(command);
                        }
                        System.out.println("[INFO] 队列已删除: " + queueName);
                        return "OK: 队列已删除";
                    } finally {
                        queue.writeLock().unlock();
                    }
                } finally {
                    structureLock.unlock();
                }
            }
            default:
                return "ERROR: 未知命令";
        }
    }

    public PersistentLogger getLogger() {
        return logger;
    }

    // 简单的持久化日志实现（写入文件）
    public static class PersistentLogger {
        private java.nio.channels.FileChannel channel;
        private Path logFilePath;

        // 保存每一行起始的字节偏移
        private final List<Long> lineOffsets = new ArrayList<>();

        // 使用读写锁，允许多个并发读取，同时写入操作独占锁
        private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

        public PersistentLogger(String filePath) throws IOException {
            this.logFilePath = Paths.get(filePath);
            channel = java.nio.channels.FileChannel.open(logFilePath,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
            loadLineOffsets();
        }

        private void loadLineOffsets() throws IOException {
            long fileSize = channel.size();
            // 如果文件为空，就不需要处理
            if (fileSize == 0) {
                return;
            }
            // 使用内存映射，注意映射的范围不能超过文件大小
            MappedByteBuffer mappedBuffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize);
            // 第一行的起始偏移量固定为0
            lineOffsets.add(0L);
            for (long pos = 0; pos < fileSize; pos++) {
                // mappedBuffer.get(int index) 需要 int 类型索引，因此强制转换
                byte b = mappedBuffer.get((int) pos);
                // 假设日志每行以 '\n' 结尾
                if (b == (byte) '\n') {
                    // 如果不是文件末尾，则下一个字节就是新行的起始偏移
                    if (pos + 1 < fileSize) {
                        lineOffsets.add(pos + 1);
                    }
                }
            }
            lineOffsets.add(fileSize);
        }

        public void log(String record) {
            rwLock.writeLock().lock();

            try {
                String line = record + "\n";
                ByteBuffer buffer = ByteBuffer.wrap(line.getBytes(StandardCharsets.UTF_8));

                while (buffer.hasRemaining()) {
                    channel.write(buffer, channel.size());
                }
                channel.force(true);
                // 记录新行的起始位置，即当前文件大小
                lineOffsets.add(channel.size());

            } catch (IOException e) {
                System.err.println("[ERROR] 写入日志失败：" + e.getMessage());
            } finally {
                rwLock.writeLock().unlock();
            }
        }

        public void close() {
            try {
                channel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // 快速读取第 n 行 (n从1开始计数)
        public String readLine(int n) throws IOException {
            rwLock.readLock().lock();
            try {
                if(n < 1 || n >= lineOffsets.size()) {
                    return null;
                }
                // 获取当前行的起始偏移
                long start = lineOffsets.get(n - 1);
                long end;
                if(n < lineOffsets.size() - 1) {
                    // 下一个行的起始偏移减去当前偏移就是这一行的字节数
                    end = lineOffsets.get(n) - 1; // 去除换行符
                } else {
                    // 如果是最后一行，则到文件末尾
                    end = channel.size();
                }
                int length = (int)(end - start);
                ByteBuffer buffer = ByteBuffer.allocate(length);
                channel.read(buffer, start);
                buffer.flip();
                return new String(buffer.array(), "UTF-8").trim();
            } finally {
                rwLock.readLock().unlock();
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
