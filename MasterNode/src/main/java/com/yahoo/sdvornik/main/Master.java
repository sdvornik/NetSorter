package com.yahoo.sdvornik.main;

import com.yahoo.sdvornik.sharable.Constants;
import com.yahoo.sdvornik.Utils;
import com.yahoo.sdvornik.sharable.BroadcastMessage;
import com.yahoo.sdvornik.server.MasterServer;
import com.yahoo.sdvornik.server.BroadcastServer;
import com.yahoo.sdvornik.server.WebSocketServer;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main class for master node
 */
public enum Master {

    INSTANCE;

    private final Logger log = LoggerFactory.getLogger(Master.class.getName());

    private final EventLoopGroup masterEventLoopGroup = new NioEventLoopGroup();

    private final EventLoopGroup wsEventLoopGroup = new NioEventLoopGroup();

    private final EventLoopGroup udpEventLoopGroup = new NioEventLoopGroup();

    private final ChannelGroup wsChannelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    private final ChannelGroup masterChannelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    public static void main(String[] args) throws Exception {

        new WebSocketServer(
                INSTANCE.wsEventLoopGroup,
                INSTANCE.wsChannelGroup
        ).init();

        new BroadcastServer(
                INSTANCE.udpEventLoopGroup
        ).init();

        new MasterServer(
                INSTANCE.masterEventLoopGroup,
                INSTANCE.masterChannelGroup
        ).init();
    }

    /**
     * Send message to all WebSocket connections
     * @param msg
     */
    public void sendMsgToWebSocketGroup(String msg) {
        wsChannelGroup.writeAndFlush(new TextWebSocketFrame(msg));
    }

    /**
     * Add {@link Channel} channel of worker node to group of worker node.
     * @param channel
     */
    public void addChannelToMasterGroup(Channel channel) {
        masterChannelGroup.add(channel);
    }

    /**
     * Remove {@link Channel} channel of worker from group.
     * @param channel
     */
    public void removeChannelFromMasterGroup(Channel channel) {
        masterChannelGroup.remove(channel);
    }

    /**
     * Add {@link Channel} WebSocket channel to group of WebSocket connections.
     * @param channel
     */
    public void addChannelToWebSocketGroup(Channel channel) {
        wsChannelGroup.add(channel);
    }

    /**
     * Remove {@link Channel} WebSocket channel from group of WebSocket connections.
     * @param channel
     */
    public void removeChannelFromWebSocketGroup(Channel channel) {
        wsChannelGroup.remove(channel);
    }

    /**
     * Getter for group of worker nodes.
     * @return
     */
    public ChannelGroup getMasterChannelGroup() {
        return masterChannelGroup;
    }

    /**
     * Shutdown master node.
     */
    public void stop() {
        ChannelGroupFuture closeWsGroupFuture = wsChannelGroup.close();
        closeWsGroupFuture.addListener(new ChannelGroupFutureListener() {
            @Override
            public void operationComplete(ChannelGroupFuture future) throws Exception {
                wsEventLoopGroup.shutdownGracefully();
                log.info("Successfully shutdown WebSocket server");
            }
        });

        ChannelGroupFuture closeMasterGroupFuture = masterChannelGroup.close();
        closeMasterGroupFuture.addListener(new ChannelGroupFutureListener() {
            @Override
            public void operationComplete(ChannelGroupFuture future) throws Exception {
                INSTANCE.masterEventLoopGroup.shutdownGracefully();
                INSTANCE.log.info("Successfully shutdown Master server");
            }
        });

        Future<?> shutdownBroadcasterEventLoop = udpEventLoopGroup.shutdownGracefully();
        shutdownBroadcasterEventLoop.awaitUninterruptibly();
        log.info("Successfully shutdown Broadcast server");
    }
}
