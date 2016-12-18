package com.yahoo.sdvornik.master;

import com.yahoo.sdvornik.Constants;
import com.yahoo.sdvornik.main.EntryPoint;
import com.yahoo.sdvornik.server.MasterServer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
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
        final String id = ctx.channel().id().asShortText();
        String joinMsg = "Worker node with id: " + id + " joined into the cluster";
        log.info(joinMsg);
        EntryPoint.sendMsgToWebSocketGroup(joinMsg);
        ChannelFuture closeFuture = ctx.channel().closeFuture();

        closeFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                EntryPoint.removeChannelFromMasterGroup(ctx.channel());
                String leftMsg = "Node with id: " + id + " left the cluster";
                log.info(leftMsg);
                EntryPoint.sendMsgToWebSocketGroup(leftMsg);
            }
        });
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf byteBuf = (ByteBuf) msg;
        int numberOfChunk = byteBuf.readInt();
        if(numberOfChunk < 0) {
            log.info("Server received: " + numberOfChunk);
            switch(numberOfChunk) {
                case Constants.GET_CONNECTION :
                    ByteBuf buf = Unpooled.buffer(Long.BYTES+Integer.BYTES);
                    buf.writeLong(Integer.BYTES);
                    buf.writeInt(Constants.CONNECTED);
                    ctx.writeAndFlush(buf);
                break;
                default :
            }
            byteBuf.release();
            return;

        }
        byteBuf.release();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) {
        log.error("Exception in Master handler", e);
        ctx.close();
    }
}
