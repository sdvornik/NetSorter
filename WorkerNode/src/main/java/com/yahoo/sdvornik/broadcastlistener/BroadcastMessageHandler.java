package com.yahoo.sdvornik.broadcastlistener;

import com.yahoo.sdvornik.worker.WorkerClient;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public class BroadcastMessageHandler extends SimpleChannelInboundHandler<BroadcastMessage> {

    private static final Logger log = LoggerFactory.getLogger(BroadcastMessageHandler.class.getName());

    @Override
    public void channelRead0(ChannelHandlerContext ctx, BroadcastMessage msg) throws Exception {
        log.info("Received message. Address: "+msg.getServerAddress().getHostAddress()+"; Port: "+msg.getServerPort());

        ChannelFuture closeConnectionFuture = ctx.close();

        closeConnectionFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if(!future.isSuccess()) {
                    log.error("Can't close Broadcast listener", future.cause());
                    return;
                }

                log.info("Successfully close Broadcast listener");
                new WorkerClient(new InetSocketAddress(msg.getServerAddress(), msg.getServerPort())).init();
            }
        });


    }


    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) throws Exception {
        log.error("Exception in BroadcastMessageHandler", e);
        ctx.close();
    }
}

