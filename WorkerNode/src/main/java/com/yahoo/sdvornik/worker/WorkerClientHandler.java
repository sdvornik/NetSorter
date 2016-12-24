package com.yahoo.sdvornik.worker;

import com.yahoo.sdvornik.broadcastlistener.BroadcastListener;
import com.yahoo.sdvornik.main.Worker;
import com.yahoo.sdvornik.merger.Merger;
import com.yahoo.sdvornik.sharable.MasterWorkerMessage;
import com.yahoo.sdvornik.sorter.QuickSort;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerClientHandler extends ChannelInboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(WorkerClientHandler.class.getName());

    private int taskCounter = 0;

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        ctx.writeAndFlush(MasterWorkerMessage.GET_CONNECTION.getByteBuf());

        ctx.channel().closeFuture().addListener(
                new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        Worker.INSTANCE.setMasterNodeChannel(null);
                        log.info("Worker node lost connection with master node. Start broadcast listener/");
                        new BroadcastListener().blockingInit();

                    }
                }
        );
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        final ByteBuf byteBuf = (ByteBuf)msg;
        final int numberOfChunk = byteBuf.readInt();
        if(numberOfChunk < 0) {
            MasterWorkerMessage enumMsg = MasterWorkerMessage.toEnum(numberOfChunk);
            switch(enumMsg) {
                case CONNECTED:
                    log.info("Worker node successfully connected to master node");
                    Worker.INSTANCE.setMasterNodeChannel(ctx.channel());
                    break;
                case START_SORTING:
                    int taskCounter = enumMsg.readIntPayload(byteBuf);
                    log.info("taskCounter "+taskCounter);
                    Merger.INSTANCE.init(taskCounter);
                    log.info("Worker node prepared for sorting operation. Number of chunnks "+taskCounter);
                    break;

                case STOP_TASK_TRANSMISSION:
                    log.info("Receive stop task transmission message");
                    //TODO implement interrupt Merger thread on timeout

                    break;
                case GET_RESULT:
                    log.info("Receive get result message");
                    Merger.INSTANCE.sendResult();

                    break;

                default:
            }
            byteBuf.release();
            return;
        }
        ++taskCounter;
        ctx.executor().execute(
                new Runnable() {
                    @Override
                    public void run() {

                        int keyAmount = byteBuf.readableBytes()/Long.BYTES;

                        long[] presortedArr = new long[keyAmount];
                        for(int i=0; i<keyAmount; ++i) {
                            presortedArr[i] = byteBuf.readLong();
                        }
                        presortedArr = new QuickSort(presortedArr).sort();
                        Merger.INSTANCE.putArrayInQueue(presortedArr);
                        byteBuf.release();
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
