package com.yahoo.sdvornik.server;

import com.yahoo.sdvornik.Constants;
import com.yahoo.sdvornik.handlers.WebSocketFrameHandler;
import com.yahoo.sdvornik.main.MasterEntryPoint;
import com.yahoo.sdvornik.handlers.HttpRequestHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * WebSocket server
 */
public class WebSocketServer {
    private static final Logger log = LoggerFactory.getLogger(WebSocketServer.class.getName());

    private final EventLoopGroup wsEventLoopGroup;

    private ChannelGroup wsChannelGroup;

    /**
     * Ctor.
     * @param wsEventLoopGroup
     * @param wsChannelGroup
     */
    public WebSocketServer(EventLoopGroup wsEventLoopGroup, ChannelGroup wsChannelGroup) {
        this.wsEventLoopGroup = wsEventLoopGroup;
        this.wsChannelGroup = wsChannelGroup;
    }

    public void init() {
        final ServerBootstrap wsBootstrap = new ServerBootstrap();
        wsBootstrap.group(wsEventLoopGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new HttpServerCodec());
                        pipeline.addLast(new HttpObjectAggregator(64 * 1024));
                        pipeline.addLast(new ChunkedWriteHandler());
                        pipeline.addLast(new HttpRequestHandler("/ws"));
                        pipeline.addLast(new WebSocketServerProtocolHandler("/ws"));
                        pipeline.addLast(new WebSocketFrameHandler());
                    }
                });

        ChannelFuture wsFuture = wsBootstrap.bind(new InetSocketAddress(Constants.WEBSOCKET_PORT));

        wsFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if(!future.isSuccess()) {
                    log.error("Can't run WebSocket server", future.cause());
                    MasterEntryPoint.INSTANCE.stop();
                    return;
                }
                log.info("Successfully init WebSocket server");
            }
        });
    }
}
