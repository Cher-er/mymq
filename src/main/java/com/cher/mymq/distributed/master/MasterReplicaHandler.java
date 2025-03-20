package com.cher.mymq.distributed.master;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.List;

public class MasterReplicaHandler extends SimpleChannelInboundHandler<String> {

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // 当从节点连接时，将该 Channel 加入全局 ChannelGroup
        MasterServer.getReplicaChannels().putIfAbsent(ctx.channel(), 1);
        System.out.println("[Master] 从节点连接建立: " + ctx.channel().remoteAddress());
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
        // 主节点不需要处理从节点发来的数据（通常仅用于心跳等用途），所以可直接忽略
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }
}
