package com.cher.mymq.distributed.slave;

import com.cher.mymq.distributed.master.MessageQueueBusinessLogic;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class SlaveReplicationHandler extends SimpleChannelInboundHandler<String> {
    private final MessageQueueBusinessLogic businessLogic;

    public SlaveReplicationHandler(MessageQueueBusinessLogic businessLogic) {
        this.businessLogic = businessLogic;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
        String command = msg.trim();
        // 从节点在同步模式下，不写日志（shouldLog = false）
        String response = businessLogic.applyCommand(command, false);
        System.out.println("[Slave] 同步命令: " + command + " -> " + response);
        // 可选：回复心跳或状态
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }
}