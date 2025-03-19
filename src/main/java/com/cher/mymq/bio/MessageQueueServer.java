package com.cher.mymq.bio;

import java.net.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

public class MessageQueueServer {
    private static final int PORT = 9999;
    private static final String LOG_FILE = "messagequeue.log";

    // 使用 ConcurrentHashMap 管理各个队列，每个队列使用 LinkedBlockingQueue 实现
    private ConcurrentHashMap<String, LinkedBlockingQueue<String>> queues = new ConcurrentHashMap<>();

    // 日志工具实例，用于持久化状态变更记录
    private PersistentLogger logger;

    private final Object opLock = new Object();

    private static boolean running = true; // 运行状态

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

    public void start() {
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("[INFO] 消息队列服务端已启动，监听端口：" + PORT);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("收到退出信号，正在关闭 MessageQueueServer...");
                stopServer();
            }));

            // 持续监听客户端连接
            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("[INFO] 接收到来自 " + clientSocket.getRemoteSocketAddress() + " 的连接");
                // 为每个客户端连接开启新线程进行处理
                new Thread(() -> handleClient(clientSocket)).start();
            }
        } catch (IOException e) {
            System.out.println("[ERROR] 服务器异常：" + e.getMessage());
            e.printStackTrace();
        } finally {
            if (logger != null) {
                logger.close();
            }
        }
    }

    private void handleClient(Socket socket) {
        try (
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true)
        ) {
            String line;
            // 持续读取客户端发送的命令
            while ((line = in.readLine()) != null) {
                System.out.println("[DEBUG] 收到命令: " + line);
                // 对命令进行处理，同时将状态变更记录写入日志（参数 true）
                String response = applyCommand(line, true);
                System.out.println("[DEBUG] 响应命令: " + response);
                out.println(response);
            }
            System.out.println("[INFO] 客户端 " + socket.getRemoteSocketAddress() + " 断开连接");
        } catch (IOException e) {
            System.out.println("[ERROR] 处理客户端 " + socket.getRemoteSocketAddress() + " 时异常：" + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 解析并应用客户端或日志中的命令
     * @param command 命令字符串（格式：PUBLISH/CONSUME/CREATE/DROP queueName [message]）
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
            case "PUBLISH":
                synchronized (opLock) {
                    if (parts.length < 3) {
                        return "ERROR: PUBLISH 命令需要消息内容";
                    }
                    String message = parts[2];
                    // 如果队列不存在则自动创建
                    queues.computeIfAbsent(queueName, k -> {
                        System.out.println("[INFO] 自动创建队列: " + k);
                        return new LinkedBlockingQueue<>();
                    }).offer(message);
                    if (shouldLog) {
                        logger.log(command);
                    }
                    System.out.println("[INFO] 消息已发布到队列 " + queueName + ": " + message);
                    return "OK: 消息已发布";
                }

            case "CONSUME":
                synchronized (opLock) {
                    LinkedBlockingQueue<String> queue = queues.get(queueName);
                    if (queue == null) {
                        return "ERROR: 队列不存在";
                    }
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
                }


            case "CREATE":
                synchronized (opLock) {
                    if (queues.containsKey(queueName)) {
                        return "ERROR: 队列已存在";
                    } else {

                        queues.put(queueName, new LinkedBlockingQueue<>());
                        if (shouldLog) {
                            logger.log(command);
                        }

                        System.out.println("[INFO] 队列已创建: " + queueName);
                        return "OK: 队列已创建";
                    }
                }

            case "DROP":
                synchronized (opLock) {
                    if (!queues.containsKey(queueName)) {
                        return "ERROR: 队列不存在";
                    } else {
                        queues.remove(queueName);
                        if (shouldLog) {
                            logger.log(command);
                        }
                        System.out.println("[INFO] 队列已删除: " + queueName);
                        return "OK: 队列已删除";
                    }
                }

            default:
                return "ERROR: 未知命令";
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
        // 在这里添加持久化、清理资源等操作
        System.out.println("MessageQueueServer 退出成功");
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

        /**
         * 将记录写入日志文件
         * @param record 记录内容
         */
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

        /**
         * 关闭日志通道
         */
        public void close() {
            try {
                channel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        /**
         * 读取日志文件中所有行
         * @param filePath 日志文件路径
         * @return 日志中每一行命令的列表
         * @throws IOException 读取异常
         */
        public static List<String> readLog(String filePath) throws IOException {
            Path path = Paths.get(filePath);
            if (!Files.exists(path)) {
                return Collections.emptyList();
            }
            return Files.readAllLines(path, StandardCharsets.UTF_8);
        }
    }
}

