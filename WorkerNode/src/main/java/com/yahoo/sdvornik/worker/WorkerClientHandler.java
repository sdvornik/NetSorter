package com.yahoo.sdvornik.worker;

import com.yahoo.sdvornik.sorter.QuickSort;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerClientHandler extends ChannelInboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(WorkerClientHandler.class.getName());

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        ctx.writeAndFlush(Unpooled.copiedBuffer("Get connection", CharsetUtil.UTF_8));
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        final ByteBuf byteBuf = (ByteBuf)msg;

        ctx.executor().execute(
                new Runnable() {
                    @Override
                    public void run() {
                        int numberOfChunk = byteBuf.readInt();
                        int keyAmount = byteBuf.readableBytes()/Long.BYTES;
                        long[] presortedArr = new long[keyAmount];
                        new QuickSort(presortedArr).sort();
                        log.info("Successfully sorted chunk number "+numberOfChunk);
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
