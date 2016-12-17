package com.yahoo.sdvornik.broadcastlistener;

import com.yahoo.sdvornik.Constants;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public class BroadcastListener {

    private static final Logger log = LoggerFactory.getLogger(BroadcastListener.class.getName());

    private final EventLoopGroup broadcastListenerGroup;

    public BroadcastListener() {
        broadcastListenerGroup = new NioEventLoopGroup();
    }

    public void init() {
        final Bootstrap bootstrap = new Bootstrap();
        final InetSocketAddress address = new InetSocketAddress(Constants.BROADCAST_PORT);
        bootstrap.group(broadcastListenerGroup)
                .channel(NioDatagramChannel.class)
                .option(ChannelOption.SO_BROADCAST, true)
                .handler( new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel channel)
                            throws Exception {
                        ChannelPipeline pipeline = channel.pipeline();
                        pipeline.addLast(new BroadcastMessageDecoder());
                        pipeline.addLast(new BroadcastMessageHandler());
                    }
                } )
                .localAddress(address);

        ChannelFuture broadcastFuture = bootstrap.bind();

        broadcastFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if(future.isSuccess()){
                    log.info("BroadcastListener successfully started");
                };

            }
        });
/*
        try {
            Channel channel = bootstrap.bind().sync().channel();
            channel.closeFuture().sync();
        }
        catch(InterruptedException e) {

        }
*/

    }

    public void stop() {
        broadcastListenerGroup.shutdownGracefully();
    }

}
