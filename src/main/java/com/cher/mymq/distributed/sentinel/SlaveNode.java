package com.cher.mymq.distributed.sentinel;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

public class SlaveNode {
    private String host;
    private int port;
    private int priority;

    public SlaveNode(String host, int port, int priority) {
        this.host = host;
        this.port = port;
        this.priority = priority;
    }

    /**
     * 检查该从节点是否存活，尝试建立 TCP 连接
     */
    public boolean isAlive() {
        try (Socket socket = new Socket(host, port);
             OutputStream os = socket.getOutputStream();
             BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            os.write("PING\n".getBytes());
            os.flush();

            String response = br.readLine();
            return "PONG".equalsIgnoreCase(response);
        } catch (Exception e) {
            return false;
        }
    }

    public int getPriority() {
        return priority;
    }

    @Override
    public String toString() {
        return "SlaveNode{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", priority=" + priority +
                '}';
    }
}

