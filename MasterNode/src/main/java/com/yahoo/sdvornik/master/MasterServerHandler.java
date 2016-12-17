package com.yahoo.sdvornik.master;

import com.yahoo.sdvornik.main.EntryPoint;
import com.yahoo.sdvornik.server.MasterServer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.group.ChannelGroup;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ChannelHandler.Sharable
public class MasterServerHandler extends ChannelInboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(MasterServerHandler.class.getName());

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        EntryPoint.addChannelToMasterGroup(ctx.channel());
        String id = ctx.channel().id().toString();
        log.info("Channel Active "+id);
        EntryPoint.sendMsgToWebSocketGroup("Node with id: " + id + " joined into the cluster");
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        EntryPoint.removeChannelFromMasterGroup(ctx.channel());
        String id = ctx.channel().id().toString();
        log.info("Channel Inactive "+id);
        EntryPoint.sendMsgToWebSocketGroup("Node with id: " + id + " left the cluster");
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf in = (ByteBuf) msg;
        log.info("Server received: " + in.toString(CharsetUtil.UTF_8));
        ctx.write(in);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) {
        log.error("Exception in Master handler", e);
        ctx.close();
    }
}
