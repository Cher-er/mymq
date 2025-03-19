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
            System.out.println("消息队列服务端已启动，监听端口：" + PORT);
            // 持续监听客户端连接
            while (true) {
                Socket clientSocket = serverSocket.accept();
                // 为每个客户端连接开启新线程进行处理
                new Thread(() -> handleClient(clientSocket)).start();
            }
        } catch (IOException e) {
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
                String response = processCommand(line);
                out.println(response);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 解析并处理客户端命令
    private String processCommand(String command) {
        // 命令格式：PUBLISH queueName message 或 CONSUME queueName
        String[] parts = command.split(" ", 3);
        if (parts.length < 2) {
            return "ERROR: 无效的命令格式";
        }
        String action = parts[0];
        String queueName = parts[1];

        switch (action.toUpperCase()) {
            case "PUBLISH":
                if (parts.length < 3) {
                    return "ERROR: PUBLISH 命令需要携带消息内容";
                }
                String message = parts[2];
                // 队列不存在时自动创建队列，并放入消息
                queues.computeIfAbsent(queueName, k -> new LinkedBlockingQueue<>()).offer(message);
                return "OK: 消息已发布";

            case "CONSUME":
                LinkedBlockingQueue<String> queue = queues.get(queueName);
                if (queue == null) {
                    return "ERROR: 队列不存在";
                }
                message = queue.poll();
                if (message == null) {
                    return "NO_MESSAGE";
                }
                return "MESSAGE: " + message;

            case "CREATE":
                // 如果队列已存在，返回错误；否则创建一个新队列
                if (queues.containsKey(queueName)) {
                    return "ERROR: 队列已存在";
                } else {
                    queues.put(queueName, new LinkedBlockingQueue<>());
                    return "OK: 队列已创建";
                }

            case "DROP":
                // 如果队列不存在，返回错误；否则删除队列
                if (!queues.containsKey(queueName)) {
                    return "ERROR: 队列不存在";
                } else {
                    queues.remove(queueName);
                    return "OK: 队列已删除";
                }

            default:
                return "ERROR: 未知命令";
        }
    }
}

