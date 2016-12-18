package com.yahoo.sdvornik.worker;

import com.yahoo.sdvornik.Constants;
import com.yahoo.sdvornik.merger.LongArrayWrapper;
import com.yahoo.sdvornik.merger.Merger;
import com.yahoo.sdvornik.sorter.QuickSort;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerClientHandler extends ChannelInboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(WorkerClientHandler.class.getName());

    private Channel masterNodeChannel;

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        ByteBuf buf = Unpooled.buffer(Long.BYTES+Integer.BYTES);
        buf.writeLong(Integer.BYTES);
        buf.writeInt(Constants.GET_CONNECTION);
        ctx.writeAndFlush(buf);
        ChannelFuture closeFuture = ctx.channel().closeFuture();

        closeFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                masterNodeChannel = null;
                String leftMsg = "Worker node lost connection with master node";
                log.info(leftMsg);
            }
        });
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        final ByteBuf byteBuf = (ByteBuf)msg;
        final int numberOfChunk = byteBuf.readInt();
        if(numberOfChunk < 0) {

            switch(numberOfChunk) {
                case Constants.CONNECTED:
                    log.info("Worker node successfully connected to master node");
                    masterNodeChannel = ctx.channel();
                    break;
                case Constants.START_SORTING:
                    Merger.INSTANCE.init();
                    log.info("Worker node prepared for sorting operation");

                default:
            }
            byteBuf.release();
            return;
        }

        ctx.executor().execute(
                new Runnable() {
                    @Override
                    public void run() {

                        int keyAmount = byteBuf.readableBytes()/Long.BYTES;
                        long[] presortedArr = new long[keyAmount];
                        log.info("Presorted array size: "+keyAmount);
                        for(int i=0; i<keyAmount; ++i) {
                            presortedArr[i] = byteBuf.readLong();
                        }
                        byteBuf.release();
                        new QuickSort(presortedArr).sort();
                        LongArrayWrapper arr = new LongArrayWrapper(0, presortedArr);
                        log.info("Successfully sorted chunk number "+numberOfChunk);
                        Merger.INSTANCE.putArrayInQueue(arr);
                    }
                }
        );
    }
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) {
        log.error("Exception in WorkerHandler", e);
        ctx.close();
    }
}
