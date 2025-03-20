package com.cher.mymq.distributed.master;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MasterClientHandler extends SimpleChannelInboundHandler<String> {
    private final MessageQueueBusinessLogic businessLogic;
    // 工作线程池，防止业务处理阻塞 I/O 线程
    private final ExecutorService workerPool = Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors()
    );

    public MasterClientHandler(MessageQueueBusinessLogic businessLogic) {
        this.businessLogic = businessLogic;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        System.out.println("[Master] 客户端连接建立: " + ctx.channel().remoteAddress());
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
        final String command = msg.trim();
        // 异步处理客户端命令
        workerPool.submit(() -> {
            String response = businessLogic.applyCommand(command, true);
            ctx.writeAndFlush(response + "\n");
        });
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}
