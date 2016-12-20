package com.yahoo.sdvornik.server;

import com.yahoo.sdvornik.sharable.Constants;
import com.yahoo.sdvornik.main.Master;
import com.yahoo.sdvornik.websocket.WebSocketServerInitializer;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
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
                .childHandler(new WebSocketServerInitializer(this));

        ChannelFuture wsFuture = wsBootstrap.bind(new InetSocketAddress(Constants.WEBSOCKET_PORT));

        wsFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if(!future.isSuccess()) {
                    log.error("Can't run WebSocket server", future.cause());
                    Master.INSTANCE.stop();
                    return;
                }
                log.info("Successfully init WebSocket server");
            }
        });
    }
}
