package com.yahoo.sdvornik.server;

import com.yahoo.sdvornik.sharable.Constants;
import com.yahoo.sdvornik.sharable.BroadcastMessage;
import com.yahoo.sdvornik.broadcaster.BroadcastMessageEncoder;
import com.yahoo.sdvornik.main.EntryPoint;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.nio.NioDatagramChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class BroadcastServer {

    private static final Logger log = LoggerFactory.getLogger(BroadcastServer.class.getName());

    private final EventLoopGroup udpEventLoopGroup;

    private BroadcastMessage broadcastMessage;

    public BroadcastServer(EventLoopGroup udpEventLoopGroup, BroadcastMessage broadcastMessage) {
        this.udpEventLoopGroup = udpEventLoopGroup;
        this.broadcastMessage = broadcastMessage;
    }

    public void init() {
        final Bootstrap udpBootstrap = new Bootstrap();

        udpBootstrap.group(udpEventLoopGroup)
                .channel(NioDatagramChannel.class)
                .option(ChannelOption.SO_BROADCAST, true)
                .handler(new BroadcastMessageEncoder());


        ChannelFuture udpFuture = udpBootstrap.bind(0);
        udpFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if(!future.isSuccess()) {
                    log.error("Can't run Broadcast server", future.cause());
                    EntryPoint.stop();
                    return;
                }
                log.info("Successfully init Broadcast server");
                final Channel udpChannel = future.channel();
                udpEventLoopGroup.scheduleAtFixedRate(
                        new Runnable() {
                            @Override
                            public void run() {
                                udpChannel.writeAndFlush(broadcastMessage);
                                //log.info("Successfully broadcast message");
                            }
                        },
                        0,
                        Constants.BROADCAST_INTERVAL_IN_MS,
                        TimeUnit.MILLISECONDS
                );
            }
        });
    }
}