package com.cher.mymq.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;

public class NettyMessageQueueServer {
    private static final int PORT = 9999;

    public static void main(String[] args) throws Exception {
        // 创建业务逻辑对象，并加载历史持久化数据
        MessageQueueServer businessLogic = new MessageQueueServer();
        businessLogic.loadPersistedData();

        // 创建 Netty 的 boss 和 worker 线程组
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline pipeline = ch.pipeline();
                            // 使用基于行分隔符的解码器和编码器
                            pipeline.addLast(new LineBasedFrameDecoder(1024));
                            pipeline.addLast(new StringDecoder(CharsetUtil.UTF_8));
                            pipeline.addLast(new StringEncoder(CharsetUtil.UTF_8));
                            // 添加自定义业务处理器
                            pipeline.addLast(new MessageQueueServerHandler(businessLogic));
                        }
                    });

            ChannelFuture future = bootstrap.bind(PORT).sync();
            System.out.println("[INFO] Netty MessageQueueServer started on port " + PORT);
            future.channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}
