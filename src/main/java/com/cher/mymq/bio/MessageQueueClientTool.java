package com.cher.mymq.bio;

import java.io.*;
import java.net.*;

/**
 * MessageQueueClientTool 封装了与消息队列服务端的交互，
 * 提供创建队列、生产消息、消费消息、删除队列等操作。
 */
public class MessageQueueClientTool {
    private Socket socket;
    private BufferedReader in;
    private PrintWriter out;

    /**
     * 构造方法：建立与服务端的连接
     *
     * @param host 服务端地址
     * @param port 服务端端口
     * @throws IOException 连接异常
     */
    public MessageQueueClientTool(String host, int port) throws IOException {
        this.socket = new Socket(host, port);
        this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.out = new PrintWriter(socket.getOutputStream(), true);
    }

    /**
     * 创建队列。如果队列已存在，将返回错误消息。
     *
     * @param queueName 队列名称
     * @return 服务端响应
     */
    public String createQueue(String queueName) {
        return sendCommand("CREATE " + queueName);
    }

    /**
     * 发布消息到指定队列。如果队列不存在，将自动创建队列后发布消息。
     *
     * @param queueName 队列名称
     * @param message   消息内容
     * @return 服务端响应
     */
    public String publish(String queueName, String message) {
        return sendCommand("PUBLISH " + queueName + " " + message);
    }

    /**
     * 从指定队列消费一条消息。
     *
     * @param queueName 队列名称
     * @return 服务端响应（如果队列为空，返回 "NO_MESSAGE"）
     */
    public String consume(String queueName) {
        return sendCommand("CONSUME " + queueName);
    }

    /**
     * 删除指定队列。如果队列不存在，将返回错误消息。
     *
     * @param queueName 队列名称
     * @return 服务端响应
     */
    public String dropQueue(String queueName) {
        return sendCommand("DROP " + queueName);
    }

    /**
     * 发送命令到服务端并等待响应
     *
     * @param command 命令字符串
     * @return 服务端响应
     */
    private String sendCommand(String command) {
        out.println(command);
        try {
            return in.readLine();
        } catch (IOException e) {
            return "ERROR: " + e.getMessage();
        }
    }

    /**
     * 关闭连接
     */
    public void close() {
        try {
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        } catch (IOException e) {
            // 输出异常信息即可
            System.out.println("关闭连接异常: " + e.getMessage());
        }
    }

    /**
     * 简单的测试方法
     */
    public static void main(String[] args) {
        try {
            MessageQueueClientTool client = new MessageQueueClientTool("localhost", 9999);

            // 测试创建队列
            System.out.println("CREATE: " + client.createQueue("testQueue"));

            // 测试发布消息
            System.out.println("PUBLISH: " + client.publish("testQueue", "Hello, World!"));

            // 测试消费消息
            System.out.println("CONSUME: " + client.consume("testQueue"));

            // 测试删除队列
            System.out.println("DROP: " + client.dropQueue("testQueue"));

            client.close();
        } catch (IOException e) {
            System.out.println("客户端异常: " + e.getMessage());
            e.printStackTrace();
        }
    }
}

