package com.cher.mymq.distributed.sentinel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Sentinel {
    // 主节点地址和端口（这里假设主节点服务运行在9999端口）
    private String masterHost = "localhost";
    private int masterPort = 9999;

    // 从节点列表，实际场景中可以动态发现或者配置
    private List<SlaveNode> slaveNodes = new ArrayList<>();

    public Sentinel() {
        // 初始化从节点列表（示例中配置了两个从节点）
        // 注意：各从节点的端口和优先级可以根据实际情况设定
        slaveNodes.add(new SlaveNode("localhost", 9998, 1));
    }

    /**
     * 启动哨兵程序，定时检测主节点状态
     */
    public void start() {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        // 每5秒检测一次主节点
        scheduler.scheduleAtFixedRate(() -> {
            if (!isMasterAlive()) {
                System.out.println("[Sentinel] 检测到主节点宕机，开始选举新的主节点...");
                SlaveNode newMaster = electNewMaster();
                if (newMaster != null) {
                    System.out.println("[Sentinel] 新的主节点选举结果: " + newMaster);
                    // TODO: 后续可以实现通知所有节点、更新客户端路由等操作
                } else {
                    System.out.println("[Sentinel] 没有可用的从节点进行提升。");
                }
            } else {
                System.out.println("[Sentinel] 主节点存活。");
            }
        }, 0, 5, TimeUnit.SECONDS);
    }

    /**
     * 检测主节点是否存活，尝试连接指定的host和port
     */
    private boolean isMasterAlive() {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(masterHost, masterPort), 2000);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * 选举一个存活的从节点作为新的主节点
     * 这里采用简单的优先级比较，优先级越高的从节点越优先被选举
     */
    private SlaveNode electNewMaster() {
        SlaveNode candidate = null;
        for (SlaveNode slave : slaveNodes) {
            if (slave.isAlive()) { // 检查从节点是否存活
                if (candidate == null || slave.getPriority() > candidate.getPriority()) {
                    candidate = slave;
                }
            }
        }
        return candidate;
    }

    public static void main(String[] args) {
        new Sentinel().start();
    }
}

