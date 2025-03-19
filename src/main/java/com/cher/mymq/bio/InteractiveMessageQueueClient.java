package com.cher.mymq.bio;

import java.io.*;
import java.net.*;

public class InteractiveMessageQueueClient {
    public static void main(String[] args) {
        // 默认连接参数，可通过命令行参数指定
        String host = "localhost";
        int port = 9999;
        if (args.length >= 1) {
            host = args[0];
        }
        if (args.length >= 2) {
            try {
                port = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                System.out.println("端口号格式错误，使用默认端口 9999");
            }
        }

        try (Socket socket = new Socket(host, port);
             // 用于读取服务器响应
             BufferedReader serverReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             // 用于向服务器发送命令（设置 autoFlush 为 true）
             PrintWriter serverWriter = new PrintWriter(socket.getOutputStream(), true);
             // 读取控制台输入
             BufferedReader consoleReader = new BufferedReader(new InputStreamReader(System.in))) {

            System.out.println("成功连接到服务器 " + host + ":" + port);
            System.out.println("请输入命令（例如 PUBLISH、CONSUME、CREATE、DROP），输入 'exit' 退出：");

            String command;
            // 循环读取用户输入的命令
            while (true) {
                System.out.print("> ");
                command = consoleReader.readLine();
                if (command == null || command.equalsIgnoreCase("exit")) {
                    break;
                }

                // 发送命令给服务端
                serverWriter.println(command);
                // 读取并打印服务端返回结果
                String response = serverReader.readLine();
                if (response != null) {
                    System.out.println("服务器响应: " + response);
                } else {
                    System.out.println("服务器关闭了连接。");
                    break;
                }
            }
            System.out.println("客户端退出。");
        } catch (IOException e) {
            System.out.println("连接异常: " + e.getMessage());
            e.printStackTrace();
        }
    }
}

