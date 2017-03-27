package com.yahoo.sdvornik.handlers;

import com.yahoo.sdvornik.clients.WorkerClient;
import com.yahoo.sdvornik.main.WorkerEntryPoint;
import com.yahoo.sdvornik.message.BroadcastMessage;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public class BroadcastMessageHandler extends SimpleChannelInboundHandler<BroadcastMessage> {

    private static final Logger log = LoggerFactory.getLogger(BroadcastMessageHandler.class);


    @Override
    public void channelRead0(ChannelHandlerContext ctx, BroadcastMessage msg) throws Exception {
        log.info(
                "Received message. Address: "+msg.getServerAddress().getHostAddress()+
                        "; Port: "+msg.getServerPort()
        );

        ctx.close().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if(!future.isSuccess()) {
                    log.error("Can't close Broadcast listener", future.cause());
                    return;
                }
                log.info("Successfully close Broadcast listener");
            }
        });

        final InetSocketAddress address = new InetSocketAddress(msg.getServerAddress(), msg.getServerPort());
        ctx.executor().execute(
                new Runnable(){

                    @Override
                    public void run() {
                        if(new WorkerClient(address).blockingInit()) {
                            WorkerEntryPoint.INSTANCE.stopBroadcastListener();
                        };
                    }
                }
        );
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) throws Exception {
        log.error("Exception in BroadcastMessageHandler", e);
        ctx.close();
    }
}

