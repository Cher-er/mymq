package com.cher.mymq.nio;

import java.net.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.*;
import java.nio.charset.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MessageQueueServer {
    private static final int PORT = 9999;
    private static final String LOG_FILE = "messagequeue.log";

    public static class QueueHolder {
        private final LinkedBlockingQueue<String> queue;
        private final ReentrantReadWriteLock lock;

        QueueHolder() {
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

        public ReentrantReadWriteLock.ReadLock readLock() {
            return lock.readLock();
        }
    }

    // 使用 ConcurrentHashMap 管理各个队列，每个队列使用 LinkedBlockingQueue 实现
    private ConcurrentHashMap<String, QueueHolder> queues = new ConcurrentHashMap<>();

    private final ReentrantLock structureLock = new ReentrantLock();

    // 日志工具实例，用于持久化状态变更记录
    private PersistentLogger logger;

    private static volatile boolean running = true; // 运行状态

    // 工作线程池，用于处理业务逻辑（applyCommand）的任务
    private ExecutorService workerPool = Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors()
    );

    public static void main(String[] args) {
        MessageQueueServer server = new MessageQueueServer();
        // 启动时加载历史持久化数据，重放日志恢复内存状态
        server.loadPersistedData();
        server.start();
    }

    public MessageQueueServer() {
        try {
            logger = new PersistentLogger(LOG_FILE);
        } catch (IOException e) {
            System.err.println("[ERROR] 初始化持久化日志失败: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 使用 NIO 实现的服务器主循环
     */
    public void start() {
        try {
            // 打开 ServerSocketChannel，并设置为非阻塞模式
            ServerSocketChannel serverChannel = ServerSocketChannel.open();
            serverChannel.configureBlocking(false);
            serverChannel.bind(new InetSocketAddress(PORT));
            System.out.println("[INFO] 消息队列服务端已启动，监听端口：" + PORT);

            // 创建 Selector 以管理多个 Channel 的 IO 事件
            Selector selector = Selector.open();
            // 注册 ServerSocketChannel 的 OP_ACCEPT 事件
            serverChannel.register(selector, SelectionKey.OP_ACCEPT);

            // 添加关闭钩子
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("收到退出信号，正在关闭 MessageQueueServer...");
                stopServer();
            }));

            // 主循环：不断等待和处理 IO 事件
            while (running) {
                selector.select();
                Iterator<SelectionKey> keyIterator = selector.selectedKeys().iterator();
                while (keyIterator.hasNext()) {
                    SelectionKey key = keyIterator.next();
                    keyIterator.remove();

                    if (!key.isValid()) {
                        continue;
                    }
                    if (key.isAcceptable()) {
                        accept(selector, key);
                    }
                    if (key.isReadable()) {
                        read(selector, key);
                    }
                    if (key.isValid() && key.isWritable()) {
                        write(key);
                    }
                }
            }
            // 清理资源
            selector.close();
            serverChannel.close();
        } catch (IOException e) {
            System.out.println("[ERROR] 服务器异常: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (logger != null) {
                logger.close();
            }
        }
    }

    /**
     * 处理连接请求
     */
    private void accept(Selector selector, SelectionKey key) throws IOException {
        ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
        SocketChannel clientChannel = serverChannel.accept();
        if (clientChannel == null) {
            return;
        }
        clientChannel.configureBlocking(false);
        System.out.println("[INFO] 接收到来自 " + clientChannel.getRemoteAddress() + " 的连接");
        // 为该连接创建一个 ClientContext，并注册读事件
        ClientContext context = new ClientContext();
        clientChannel.register(selector, SelectionKey.OP_READ, context);
    }

    /**
     * 读取客户端发送的数据，并按行处理
     */
    private void read(Selector selector, SelectionKey key) {
        SocketChannel channel = (SocketChannel) key.channel();
        ClientContext context = (ClientContext) key.attachment();
        ByteBuffer buffer = context.readBuffer;
        int bytesRead = -1;
        try {
            bytesRead = channel.read(buffer);
        } catch (IOException e) {
            System.err.println("[ERROR] 读取客户端数据失败: " + e.getMessage());
            closeChannel(key);
            return;
        }
        if (bytesRead == -1) {
            // 客户端关闭了连接
            closeChannel(key);
            return;
        }
        buffer.flip();
        CharBuffer charBuffer;
        try {
            charBuffer = context.decoder.decode(buffer);
        } catch (CharacterCodingException e) {
            e.printStackTrace();
            buffer.clear();
            return;
        }
        String received = charBuffer.toString();
        buffer.clear();

        // 将读取的内容追加到累积缓冲区中
        context.requestBuffer.append(received);
        String data = context.requestBuffer.toString();
        int index;
        // 按换行符拆分完整的命令行
        while ((index = data.indexOf("\n")) != -1) {
            String line = data.substring(0, index).trim();
            data = data.substring(index + 1);
            if (!line.isEmpty()) {
                System.out.println("[DEBUG] 收到命令: " + line);
                // 提交到工作线程池异步处理
                final String command = line;
                final SelectionKey currentKey = key;
                workerPool.submit(() -> {
                    String response = applyCommand(command, true);
                    System.out.println("[DEBUG] 响应命令: " + response);
                    // 将响应入队，并设置写事件兴趣
                    synchronized (context) {
                        context.enqueueResponse(response + "\n");
                    }
                    currentKey.interestOps(currentKey.interestOps() | SelectionKey.OP_WRITE);
                    // 唤醒阻塞的 select() 调用
                    selector.wakeup();
                });

            }
        }
        // 保存未处理的部分
        context.requestBuffer.setLength(0);
        context.requestBuffer.append(data);
    }

    /**
     * 向客户端写入响应数据
     */
    private void write(SelectionKey key) {
        SocketChannel channel = (SocketChannel) key.channel();
        ClientContext context = (ClientContext) key.attachment();
        Queue<ByteBuffer> queue = context.writeQueue;
        try {
            while (!queue.isEmpty()) {
                ByteBuffer buf = queue.peek();
                channel.write(buf);
                if (buf.hasRemaining()) {
                    // 未写完则退出，等待下次写事件
                    break;
                }
                queue.poll();
            }
            if (queue.isEmpty()) {
                // 写完后取消写事件兴趣
                key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
            }
        } catch (IOException e) {
            System.err.println("[ERROR] 写入客户端数据失败: " + e.getMessage());
            closeChannel(key);
        }
    }

    /**
     * 关闭客户端连接
     */
    private void closeChannel(SelectionKey key) {
        try {
            key.channel().close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        key.cancel();
    }

    /**
     * 解析并应用客户端或日志中的命令
     *
     * @param command   命令字符串（格式：PUBLISH/CONSUME/CREATE/DROP queueName [message]）
     * @param shouldLog 是否记录该命令到日志（重放日志时传 false）
     * @return 操作结果响应
     */
    private String applyCommand(String command, boolean shouldLog) {
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
                    // 如果队列不存在则自动创建
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
                        // 记录消费操作，确保重放时也删除对应消息
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
     * 加载持久化日志数据，重放每条命令恢复内存队列状态
     */
    public void loadPersistedData() {
        System.out.println("[INFO] 正在加载持久化数据...");
        try {
            List<String> commands = PersistentLogger.readLog(LOG_FILE);
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

    private static void stopServer() {
        running = false;
        System.out.println("MessageQueueServer 退出成功");
    }

    /**
     * 内部类：用于保存每个客户端连接的状态信息
     */
    private static class ClientContext {
        // 读取缓冲区
        ByteBuffer readBuffer = ByteBuffer.allocate(1024);
        // 用于累积接收到的文本数据
        StringBuilder requestBuffer = new StringBuilder();
        // 待发送的响应队列
        Queue<ByteBuffer> writeQueue = new LinkedList<>();
        // 字符集解码器
        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();

        void enqueueResponse(String response) {
            ByteBuffer buf = ByteBuffer.wrap(response.getBytes(StandardCharsets.UTF_8));
            writeQueue.offer(buf);
        }
    }

    /**
     * 内部类：持久化日志工具，基于 Java NIO 的 FileChannel 以追加方式写入日志文件
     */
    public static class PersistentLogger {
        private FileChannel channel;
        private Path logFilePath;

        public PersistentLogger(String filePath) throws IOException {
            this.logFilePath = Paths.get(filePath);
            // 以追加方式打开日志文件（不存在则创建）
            channel = FileChannel.open(logFilePath, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        }

        public void log(String record) {
            String line = record + "\n";
            ByteBuffer buffer = ByteBuffer.wrap(line.getBytes(StandardCharsets.UTF_8));
            try {
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                // force(true) 确保数据刷入磁盘
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

