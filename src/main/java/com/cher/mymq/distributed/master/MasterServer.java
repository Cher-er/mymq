package com.cher.mymq.distributed.master;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.GlobalEventExecutor;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

public class MasterServer {
    private static final int CLIENT_PORT = 9999;
    private static final int REPLICA_PORT = 8888;
    private static final String LOG_FILE_PATH = "messagequeue_01.log";
    // 全局 ChannelGroup 保存所有从节点连接
    private static final ConcurrentMap<Channel, Integer> replicaChannels = new ConcurrentHashMap<>();
    // 共享的业务逻辑对象
    private static final MessageQueueBusinessLogic businessLogic = new MessageQueueBusinessLogic(LOG_FILE_PATH);

    public static void main(String[] args) throws Exception {
        // 启动复制线程，确保消息严格按顺序发送到从节点
        startReplicationThread();

        // 启动客户端服务
        EventLoopGroup bossGroup1 = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup1 = new NioEventLoopGroup();
        ServerBootstrap clientBootstrap = new ServerBootstrap();
        clientBootstrap.group(bossGroup1, workerGroup1)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new LineBasedFrameDecoder(1024));
                        pipeline.addLast(new StringDecoder(CharsetUtil.UTF_8));
                        pipeline.addLast(new StringEncoder(CharsetUtil.UTF_8));
                        // 客户端处理器
                        pipeline.addLast(new MasterClientHandler(businessLogic));
                    }
                });
        ChannelFuture clientFuture = clientBootstrap.bind(CLIENT_PORT).sync();
        System.out.println("[Master] 客户端服务已启动，监听端口 " + CLIENT_PORT);

        // 启动从节点复制服务
        EventLoopGroup bossGroup2 = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup2 = new NioEventLoopGroup();
        ServerBootstrap replicaBootstrap = new ServerBootstrap();
        replicaBootstrap.group(bossGroup2, workerGroup2)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new LineBasedFrameDecoder(1024));
                        pipeline.addLast(new StringDecoder(CharsetUtil.UTF_8));
                        pipeline.addLast(new StringEncoder(CharsetUtil.UTF_8));
                        // 从节点连接处理器，将连接加入 ChannelGroup
                        pipeline.addLast(new MasterReplicaHandler());
                    }
                });
        ChannelFuture replicaFuture = replicaBootstrap.bind(REPLICA_PORT).sync();
        System.out.println("[Master] 从节点复制服务已启动，监听端口 " + REPLICA_PORT);

        // 当客户端服务关闭时，关闭所有服务
        clientFuture.channel().closeFuture().sync();
        replicaFuture.channel().closeFuture().sync();

        bossGroup1.shutdownGracefully();
        workerGroup1.shutdownGracefully();
        bossGroup2.shutdownGracefully();
        workerGroup2.shutdownGracefully();
    }

    /**
     * 复制线程，按 FIFO 顺序同步命令到所有从节点
     */
    private static void startReplicationThread() {
        new Thread(() -> {

            while (true) {
                replicaChannels.forEach((channel, cursor) -> {
                    try {
                        String line = businessLogic.getLogger().readLine(cursor);
                        if (line != null) {
                            channel.writeAndFlush(line + '\n');
                            replicaChannels.compute(channel, (ch, cur) -> (cur == null ? 0 : cur + 1));
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });

            }
        }, "ReplicationThread").start();
    }

    // 提供一个访问 ChannelGroup 的方法（MasterReplicaHandler 中使用）
    public static ConcurrentMap<Channel, Integer> getReplicaChannels() {
        return replicaChannels;
    }
}
