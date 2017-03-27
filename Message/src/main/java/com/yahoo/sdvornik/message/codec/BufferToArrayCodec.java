package com.yahoo.sdvornik.message.codec;

import com.yahoo.sdvornik.message.DataMessageArray;
import com.yahoo.sdvornik.message.DataMessageBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


public class BufferToArrayCodec extends MessageToMessageCodec<DataMessageBuffer,DataMessageArray> {

    private static final Logger log = LoggerFactory.getLogger(BufferToArrayCodec.class);

    @Override
    protected void encode(ChannelHandlerContext ctx, DataMessageArray msg, List<Object> out) throws Exception {
        long[] arr = msg.getArray();

        ByteBuf buf = Unpooled.buffer(arr.length*Long.BYTES);
        for(int i = 0; i < arr.length; ++i) {
            buf.writeLong(arr[i]);
        }
        DataMessageBuffer dataMsgBuf = new DataMessageBuffer(buf, msg.getChunkNumber());
        out.add(dataMsgBuf);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, DataMessageBuffer msg, List<Object> out) throws Exception {

        ByteBuf buf = msg.getNettyBuffer();
        int length = buf.readableBytes()/Long.BYTES;
        long[] arr = new long[length];

        for(int i = 0; i < length; ++i) {
            arr[i] = buf.readLong();
        }
        buf.release();

        out.add(new DataMessageArray(arr, msg.getChunkNumber()));
    }
}
