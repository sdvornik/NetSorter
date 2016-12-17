package com.yahoo.sdvornik.worker;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;


public class WorkerClient {

    private static final Logger log = LoggerFactory.getLogger(WorkerClient.class.getName());

    private final InetSocketAddress address;

    private final EventLoopGroup workerGroup;


    public WorkerClient(InetSocketAddress address) {
        this.address = address;
        this.workerGroup = new NioEventLoopGroup();
    }

    public void init() {

        Bootstrap workerBootstrap = new Bootstrap();

        workerBootstrap.group(workerGroup)
                .channel(NioSocketChannel.class)
                .remoteAddress(address)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new WorkerClientHandler());
                    }
                });
        ChannelFuture workerFuture = workerBootstrap.connect();

        workerFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if(!future.isSuccess()) {
                    log.info("Can't run Worker client");
                    return;
                }

                log.info("Successfully init Worker client");
            }
        });

    }

    public void stop() {
        workerGroup.shutdownGracefully();
    }

}
