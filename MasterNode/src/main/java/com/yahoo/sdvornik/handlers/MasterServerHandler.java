package com.yahoo.sdvornik.handlers;

import com.yahoo.sdvornik.main.MasterEntryPoint;
import com.yahoo.sdvornik.master.MasterTask;
import com.yahoo.sdvornik.message.Message;
import com.yahoo.sdvornik.message.DataMessageArray;
import io.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ChannelHandler.Sharable
public class MasterServerHandler extends ChannelInboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(MasterServerHandler.class);

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        final String id = ctx.channel().id().asShortText();
        String joinMsg = "Worker node with id: " + id + " try to join into cluster.";
        log.info(joinMsg);
        MasterEntryPoint.INSTANCE.sendMsgToWebSocketGroup(joinMsg);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {

        if(msg instanceof Message) {
            Message.Type type = ((Message) msg).getType();
            switch (type) {
                case GET_CONNECTION:
                    log.info("Receive GET_CONNECTION_MESSAGE. Send CONNECTED_MESSAGE.");
                    final String id = ctx.channel().id().asShortText();

                    ChannelFuture future = ctx.writeAndFlush(Message.getSimpleOutboundMessage(Message.Type.CONNECTED));
                    future.addListener(new ChannelFutureListener() {
                           @Override
                           public void operationComplete(ChannelFuture future) throws Exception {
                               MasterEntryPoint.INSTANCE.addChannelToMasterGroup(ctx.channel());
                               String joinMsg = "Worker node with id: " + id + " joined into cluster.";
                               log.info(joinMsg);
                               MasterEntryPoint.INSTANCE.sendMsgToWebSocketGroup(joinMsg);
                               ChannelFuture closeFuture = ctx.channel().closeFuture();

                               closeFuture.addListener(new ChannelFutureListener() {
                                   @Override
                                   public void operationComplete(ChannelFuture future) throws Exception {
                                       MasterEntryPoint.INSTANCE.removeChannelFromMasterGroup(ctx.channel());
                                       String leftMsg = "Node with id: " + id + " left the cluster.";
                                       log.info(leftMsg);
                                       MasterEntryPoint.INSTANCE.sendMsgToWebSocketGroup(leftMsg);
                                   }
                               });
                           }
                       }
                    );
                    break;
                case JOB_ENDED:
                    log.info("Receive JOB_ENDED_MESSAGE. Save this fact.");
                    MasterTask.INSTANCE.saveResponse(ctx.channel().id().asShortText());
                    break;
            }
        }
        else if(msg instanceof DataMessageArray) {
            DataMessageArray dataMsg = (DataMessageArray) msg;
            MasterTask.INSTANCE.putArrayInQueue(
                    ctx.channel().id().asShortText(),
                    dataMsg.getChunkNumber(),
                    dataMsg.getArray()
            );
        }

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) {
        log.error("Exception in MasterEntryPoint handler", e);
        ctx.close();
    }
}
