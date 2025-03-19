package com.cher.mymq;

import java.net.*;
import java.io.*;
import java.util.concurrent.*;

public class MessageQueueServer {
    private static final int PORT = 9999;
    // 使用 ConcurrentHashMap 管理各个队列，每个队列使用 LinkedBlockingQueue 实现
    private ConcurrentHashMap<String, LinkedBlockingQueue<String>> queues = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        new MessageQueueServer().start();
    }

    public void start() {
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("[INFO] 消息队列服务端已启动，监听端口：" + PORT);
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
                String response = processCommand(line);
                System.out.println("[DEBUG] 响应命令: " + response);
                out.println(response);
            }
            System.out.println("[INFO] 客户端 " + socket.getRemoteSocketAddress() + " 断开连接");
        } catch (IOException e) {
            System.out.println("[ERROR] 处理客户端 " + socket.getRemoteSocketAddress() + " 时异常：" + e.getMessage());
            e.printStackTrace();
        }
    }

    // 解析并处理客户端命令，支持以下命令：
    // 1. PUBLISH queueName message: 如果队列不存在，则自动创建后发布消息
    // 2. CONSUME queueName: 消费指定队列中的一条消息
    // 3. CREATE queueName: 创建队列，如果队列已存在则返回错误
    // 4. DROP queueName: 删除队列，如果队列不存在则返回错误
    private String processCommand(String command) {
        // 按空格分割命令，最多分成3段（对于 PUBLISH 命令需要消息内容可能包含空格）
        String[] parts = command.split(" ", 3);
        if (parts.length < 2) {
            System.out.println("[WARN] 命令格式错误: " + command);
            return "ERROR: 无效的命令格式";
        }
        String action = parts[0];
        String queueName = parts[1];

        switch (action.toUpperCase()) {
            case "PUBLISH":
                if (parts.length < 3) {
                    System.out.println("[WARN] PUBLISH 命令缺少消息内容: " + command);
                    return "ERROR: PUBLISH 命令需要携带消息内容";
                }
                String message = parts[2];
                // 队列不存在时自动创建队列，并放入消息
                queues.computeIfAbsent(queueName, k -> {
                    System.out.println("[INFO] 自动创建队列: " + queueName);
                    return new LinkedBlockingQueue<>();
                }).offer(message);
                System.out.println("[INFO] 消息已发布到队列 " + queueName + " : " + message);
                return "OK: 消息已发布";

            case "CONSUME":
                LinkedBlockingQueue<String> queue = queues.get(queueName);
                if (queue == null) {
                    System.out.println("[WARN] 消费时队列不存在: " + queueName);
                    return "ERROR: 队列不存在";
                }
                message = queue.poll();
                if (message == null) {
                    System.out.println("[INFO] 队列 " + queueName + " 中无消息可消费");
                    return "NO_MESSAGE";
                }
                System.out.println("[INFO] 消息从队列 " + queueName + " 中被消费: " + message);
                return "MESSAGE: " + message;

            case "CREATE":
                // 如果队列已存在，返回错误；否则创建一个新队列
                if (queues.containsKey(queueName)) {
                    System.out.println("[WARN] CREATE 命令失败，队列已存在: " + queueName);
                    return "ERROR: 队列已存在";
                } else {
                    queues.put(queueName, new LinkedBlockingQueue<>());
                    System.out.println("[INFO] 队列已创建: " + queueName);
                    return "OK: 队列已创建";
                }

            case "DROP":
                // 如果队列不存在，返回错误；否则删除队列
                if (!queues.containsKey(queueName)) {
                    System.out.println("[WARN] DROP 命令失败，队列不存在: " + queueName);
                    return "ERROR: 队列不存在";
                } else {
                    queues.remove(queueName);
                    System.out.println("[INFO] 队列已删除: " + queueName);
                    return "OK: 队列已删除";
                }

            default:
                System.out.println("[WARN] 收到未知命令: " + command);
                return "ERROR: 未知命令";
        }
    }
}

