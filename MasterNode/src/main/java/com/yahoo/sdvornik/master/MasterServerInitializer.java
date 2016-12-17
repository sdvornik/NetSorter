package com.yahoo.sdvornik.master;

import com.yahoo.sdvornik.server.MasterServer;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.socket.SocketChannel;

public class MasterServerInitializer extends ChannelInitializer<SocketChannel> {

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addLast(new MasterServerHandler());
    }
}

