package com.yahoo.sdvornik.master;

import com.yahoo.sdvornik.sharable.Constants;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

public class MasterServerInitializer extends ChannelInitializer<SocketChannel> {

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addLast(
                new LengthFieldBasedFrameDecoder(
                        2* Constants.DEFAULT_CHUNK_SIZE_IN_KEYS*Long.BYTES,
                        0,
                        Long.BYTES,
                        0,
                        Long.BYTES
                )
        );
        ch.pipeline().addLast(new MasterServerHandler());
    }
}

