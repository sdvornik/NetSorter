package com.yahoo.sdvornik.server;

import com.yahoo.sdvornik.handlers.MasterServerHandler;
import com.yahoo.sdvornik.message.codec.BufferToArrayCodec;
import com.yahoo.sdvornik.message.codec.ByteBufToMsgDecoder;
import com.yahoo.sdvornik.Constants;
import com.yahoo.sdvornik.main.MasterEntryPoint;
import com.yahoo.sdvornik.message.codec.MsgToByteEncoder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * MasterEntryPoint server
 */
public class MasterServer {

    private static final Logger log = LoggerFactory.getLogger(MasterServer.class.getName());

    private final EventLoopGroup masterEventLoopGroup;

    private final ChannelGroup masterChannelGroup;

    /**
     * Ctor.
     * @param masterEventLoopGroup
     * @param masterChannelGroup
     */
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
                .localAddress(new InetSocketAddress(Constants.PORT))
                .childHandler(new ChannelInitializer<SocketChannel>(){

                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {

                        ch.pipeline().addLast(
                                new LengthFieldBasedFrameDecoder(
                                        2* Constants.DEFAULT_CHUNK_SIZE_IN_KEYS*Long.BYTES,
                                        0,
                                        Long.BYTES,
                                        0,
                                        Long.BYTES
                                )
                        );

                        ch.pipeline().addLast(new ByteBufToMsgDecoder());
                        ch.pipeline().addLast(new MsgToByteEncoder());
                        ch.pipeline().addLast(new BufferToArrayCodec());
                        ch.pipeline().addLast(new MasterServerHandler());
                    }
                });

        ChannelFuture masterFuture = masterBootstrap.bind();

        masterFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (!future.isSuccess()) {
                    log.error("Can't run MasterEntryPoint server", future.cause());
                    MasterEntryPoint.INSTANCE.stop();
                    return;
                }

                log.info("Successfully init MasterEntryPoint server");
            }
        });

    }
}
