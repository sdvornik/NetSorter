package com.yahoo.sdvornik.message.codec;

import com.yahoo.sdvornik.message.DataMessageArray;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class ShuffleDecoder extends MessageToMessageDecoder<DataMessageArray> {

    private final static ThreadLocalRandom random = ThreadLocalRandom.current();

    @Override
    protected void decode(ChannelHandlerContext ctx, DataMessageArray msg, List<Object> out) throws Exception {
        long[] a = msg.getArray();
        for(int i = a.length-1; i>=0; --i) {
            int j = random.nextInt(i+1);
            long tmp = a[j];
            a[j] = a[i];
            a[i] = tmp;
        }
        out.add(msg);
    }
}
