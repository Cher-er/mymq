package com.cher.mymq;

import java.io.*;
import java.net.*;

public class MessageQueueClient {
    private String host;
    private int port;

    public MessageQueueClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    // 发送单次请求（命令），适用于简单场景
    public String sendCommand(String command) {
        try (Socket socket = new Socket(host, port);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
            out.println(command);
            return in.readLine();
        } catch (IOException e) {
            e.printStackTrace();
            return "ERROR: " + e.getMessage();
        }
    }

    public static void main(String[] args) {
        MessageQueueClient client = new MessageQueueClient("localhost", 9999);

        // 测试 CREATE 命令
        String createResponse = client.sendCommand("CREATE myQueue");
        System.out.println("CREATE 返回：" + createResponse);

        // 测试 PUBLISH 命令（此处如果队列已存在，直接发布消息）
        String publishResponse = client.sendCommand("PUBLISH myQueue HelloWorld");
        System.out.println("PUBLISH 返回：" + publishResponse);

        // 测试 CONSUME 命令
        String consumeResponse = client.sendCommand("CONSUME myQueue");
        System.out.println("CONSUME 返回：" + consumeResponse);

        // 测试 DROP 命令
        String dropResponse = client.sendCommand("DROP myQueue");
        System.out.println("DROP 返回：" + dropResponse);
    }
}

