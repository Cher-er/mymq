package com.cher.mymq.distributed.slave;

import com.cher.mymq.distributed.master.MessageQueueBusinessLogic;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;

import java.util.concurrent.TimeUnit;

public class SlaveReplicationClient {
    private static final String MASTER_HOST = "localhost";
    private static final int MASTER_PORT = 8888;

    private static final String LOG_FILE_PATH = "messagequeue_02.log";
    // 本地业务逻辑对象
    private static final MessageQueueBusinessLogic businessLogic = new MessageQueueBusinessLogic(LOG_FILE_PATH);

    private static final EventLoopGroup group = new NioEventLoopGroup();
    private static final Bootstrap bootstrap = new Bootstrap();

    private static final int HEALTH_PORT = 9999; // 哨兵用于探测的端口

    public static void main(String[] args) throws Exception {
        configureBootstrap();
        connectToMaster();
        new Thread(SlaveHealthCheckServer::startHealthCheckServer).start();
    }

    private static void configureBootstrap() {
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new LineBasedFrameDecoder(1024));
                        pipeline.addLast(new StringDecoder(CharsetUtil.UTF_8));
                        pipeline.addLast(new StringEncoder(CharsetUtil.UTF_8));
                        pipeline.addLast(new SlaveReplicationHandler(businessLogic));
                    }
                })
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000) // 连接超时 5s
                .option(ChannelOption.SO_KEEPALIVE, true);
    }

    private static void connectToMaster() {
        bootstrap.connect(MASTER_HOST, MASTER_PORT).addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                System.out.println("[Slave] 成功连接到 Master " + MASTER_HOST + ":" + MASTER_PORT);
                future.channel().closeFuture().addListener(closeFuture -> {
                    System.out.println("[Slave] 连接断开，尝试重连...");
                    scheduleReconnect();
                });
            } else {
                System.out.println("[Slave] 连接 Master 失败，稍后重试...");
                scheduleReconnect();
            }
        });
    }

    private static void scheduleReconnect() {
        group.schedule(() -> {
            System.out.println("[Slave] 尝试重新连接 Master...");
            connectToMaster();
        }, 5, TimeUnit.SECONDS); // 5 秒后尝试重连
    }
}
