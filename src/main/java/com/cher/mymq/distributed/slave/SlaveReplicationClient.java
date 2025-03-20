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

public class SlaveReplicationClient {
    private static final String MASTER_HOST = "localhost";
    private static final int MASTER_PORT = 8888;

    private static final String LOG_FILE_PATH = "messagequeue_02.log";
    // 本地业务逻辑对象
    private static final MessageQueueBusinessLogic businessLogic = new MessageQueueBusinessLogic(LOG_FILE_PATH);

    public static void main(String[] args) throws Exception {
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(Channel ch) throws Exception {
                            ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast(new LineBasedFrameDecoder(1024));
                            pipeline.addLast(new StringDecoder(CharsetUtil.UTF_8));
                            pipeline.addLast(new StringEncoder(CharsetUtil.UTF_8));
                            pipeline.addLast(new SlaveReplicationHandler(businessLogic));
                        }
                    });
            ChannelFuture future = bootstrap.connect(MASTER_HOST, MASTER_PORT).sync();
            System.out.println("[Slave] 成功连接到 Master " + MASTER_HOST + ":" + MASTER_PORT);
            future.channel().closeFuture().sync();
        } finally {
            group.shutdownGracefully();
        }
    }
}
