package com.cher.mymq.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MessageQueueServerHandler extends SimpleChannelInboundHandler<String> {
    private final MessageQueueServer businessLogic;
    // 为业务逻辑处理建立工作线程池，避免阻塞 I/O 线程
    private final ExecutorService workerPool = Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors()
    );

    public MessageQueueServerHandler(MessageQueueServer businessLogic) {
        this.businessLogic = businessLogic;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
        final String command = msg.trim();
        // 将命令提交到工作线程池处理
        workerPool.submit(() -> {
            String response = businessLogic.applyCommand(command, true);
            // 回写响应，末尾添加换行符以匹配客户端的行解码
            ctx.writeAndFlush(response + "\n");
        });
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }
}
