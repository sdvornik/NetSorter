package com.yahoo.sdvornik.master;

import com.yahoo.sdvornik.sharable.Constants;
import com.yahoo.sdvornik.main.EntryPoint;
import com.yahoo.sdvornik.sharable.MasterWorkerMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
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
            MasterWorkerMessage elm = MasterWorkerMessage.toEnum(numberOfChunk);
            switch(elm) {
                case GET_CONNECTION :
                    ctx.writeAndFlush(MasterWorkerMessage.CONNECTED.getByteBuf());
                    break;
                case JOB_ENDED :
                    log.info("Job ended message receive");
                    MasterTask.INSTANCE.saveResponse();
                default :
            }
            byteBuf.release();
            return;

        }
        ctx.executor().execute(
                new Runnable() {
                    @Override
                    public void run() {

                        int keyAmount = byteBuf.readableBytes()/Long.BYTES;
                        long[] sortedArr = new long[keyAmount];
                        for(int i=0; i<keyAmount; ++i) {
                            sortedArr[i] = byteBuf.readLong();
                        }
                        byteBuf.release();
                        MasterTask.INSTANCE.putArrayInQueue(
                                ctx.channel().id().asShortText(),
                                numberOfChunk,
                                sortedArr
                        );

                    }
                }
        );
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) {
        log.error("Exception in Master handler", e);
        ctx.close();
    }
}
