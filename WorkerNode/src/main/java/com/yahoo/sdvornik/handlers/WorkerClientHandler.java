package com.yahoo.sdvornik.handlers;

import com.yahoo.sdvornik.clients.BroadcastListener;
import com.yahoo.sdvornik.main.WorkerEntryPoint;
import com.yahoo.sdvornik.sorter.merger.Merger;
import com.yahoo.sdvornik.message.Message;
import com.yahoo.sdvornik.message.DataMessageArray;
import com.yahoo.sdvornik.message.StartSortingMessage;
import com.yahoo.sdvornik.sorter.QuickSort;
import io.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class WorkerClientHandler extends ChannelInboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(WorkerClientHandler.class.getName());

    private final ReentrantLock connectionLock;

    private final Condition condition;

    private int taskCounter = 0;

    public WorkerClientHandler(ReentrantLock connectionLock, Condition condition) {
        this.connectionLock = connectionLock;
        this.condition = condition;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {

        ctx.channel().closeFuture().addListener(
                new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        WorkerEntryPoint.INSTANCE.setMasterNodeChannel(null);
                        log.info("WorkerEntryPoint node lost connection with master node. Start broadcast listener.");
                        new BroadcastListener().blockingInit();
                    }
                }
        );
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if(msg instanceof Message) {
            Message.Type type = ((Message) msg).getType();
            switch (type) {
                case CONNECTED:
                    log.info("Worker node successfully join to cluster.");
                    try {
                        connectionLock.lock();
                        condition.signalAll();
                    }
                    finally {
                        connectionLock.unlock();
                    }
                    WorkerEntryPoint.INSTANCE.setMasterNodeChannel(ctx.channel());
                    break;
                case TASK_TRANSMISSION_ENDED:
                    log.info("Receive stop task transmission message");
                    //TODO implement interrupt Merger thread on timeout
                    break;
                case GET_RESULT:
                    log.info("Receive get result message");
                    Merger.INSTANCE.sendResult();
                    break;
                case START_SORTING:
                    int taskCounter = ((StartSortingMessage)msg).getContent();
                    Merger.INSTANCE.init(taskCounter);
                    log.info("Worker node prepared for sorting operation. Number of chunks "+taskCounter);
                    break;
            }
        }
        else if(msg instanceof DataMessageArray) {
            ++taskCounter;

            ctx.executor().execute(
                    new Runnable() {
                        @Override
                        public void run() {
                            long[] presortedArr = ((DataMessageArray) msg).getArray();
                            presortedArr = new QuickSort(presortedArr).sort();
                            Merger.INSTANCE.putArrayInQueue(presortedArr);
                        }
                    }
            );

        }
    }
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) {
        log.error("Exception in WorkerHandler", e);
        ctx.close();
    }
}
