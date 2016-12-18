package com.yahoo.sdvornik.server;

import com.yahoo.sdvornik.Constants;
import com.yahoo.sdvornik.main.EntryPoint;
import com.yahoo.sdvornik.master.MasterServerInitializer;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public class MasterServer {
    private static final Logger log = LoggerFactory.getLogger(MasterServer.class.getName());

    private final EventLoopGroup masterEventLoopGroup;

    private final ChannelGroup masterChannelGroup;

    public MasterServer(EventLoopGroup masterEventLoopGroup, ChannelGroup masterChannelGroup) {
        this.masterEventLoopGroup = masterEventLoopGroup;
        this.masterChannelGroup = masterChannelGroup;
    }

    public void init() throws Exception {
        final ServerBootstrap masterBootstrap = new ServerBootstrap();
        masterBootstrap.group(masterEventLoopGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                //.option(ChannelOption.SO_SNDBUF, 2*Constants.DEFAULT_CHUNK_SIZE_IN_KEYS*Long.BYTES)
                .localAddress(new InetSocketAddress(Constants.PORT))
                .childHandler(new MasterServerInitializer());
        ChannelFuture masterFuture = masterBootstrap.bind();

        masterFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (!future.isSuccess()) {
                    log.error("Can't run Master server", future.cause());
                    EntryPoint.stop();
                    return;
                }

                log.info("Successfully init Master server");
            }
        });

    }
}
