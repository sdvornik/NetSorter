package com.yahoo.sdvornik.main;

import com.yahoo.sdvornik.Constants;
import com.yahoo.sdvornik.Utils;
import com.yahoo.sdvornik.broadcaster.BroadcastMessage;
import com.yahoo.sdvornik.server.MasterServer;
import com.yahoo.sdvornik.server.BroadcastServer;
import com.yahoo.sdvornik.server.WebSocketServer;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class EntryPoint {

    private static final Logger log = LoggerFactory.getLogger(EntryPoint.class.getName());

    private static EventLoopGroup masterEventLoopGroup = new NioEventLoopGroup();

    private static EventLoopGroup wsEventLoopGroup = new NioEventLoopGroup();

    private static EventLoopGroup udpEventLoopGroup = new NioEventLoopGroup();

    private static ChannelGroup wsChannelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    private static ChannelGroup masterChannelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    public static void main(String[] args) throws Exception {

        BroadcastMessage broadcastMessage = new BroadcastMessage(Utils.getLocalHostAddress(), Constants.PORT);

        WebSocketServer wsServer = new WebSocketServer(wsEventLoopGroup, wsChannelGroup);
        wsServer.init();

        BroadcastServer broadcaster = new BroadcastServer(udpEventLoopGroup, broadcastMessage);
        broadcaster.init();

        MasterServer masterServer = new MasterServer(masterEventLoopGroup, masterChannelGroup);
        masterServer.init();
    }

    public static void sendMsgToWebSocketGroup(String msg) {
        wsChannelGroup.writeAndFlush(new TextWebSocketFrame(msg));
    }

    public static void addChannelToMasterGroup(Channel channel) {
        masterChannelGroup.add(channel);
    }

    public static void removeChannelFromMasterGroup(Channel channel) {
        masterChannelGroup.remove(channel);
    }

    public static void addChannelToWebSocketGroup(Channel channel) {
        wsChannelGroup.add(channel);
    }

    public static void removeChannelFromWebSocketGroup(Channel channel) {
        wsChannelGroup.remove(channel);
    }

    public static ChannelGroup getMasterChannelGroup() {
        return masterChannelGroup;
    }

    public static void stop() {
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
                masterEventLoopGroup.shutdownGracefully();
                log.info("Successfully shutdown Master server");
            }
        });

        Future<?> shutdownBroadcasterEventLoop = udpEventLoopGroup.shutdownGracefully();
        shutdownBroadcasterEventLoop.awaitUninterruptibly();
        log.info("Successfully shutdown Broadcast server");
    }
}
