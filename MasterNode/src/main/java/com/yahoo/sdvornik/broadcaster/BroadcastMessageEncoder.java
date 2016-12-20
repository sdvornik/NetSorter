package com.yahoo.sdvornik.broadcaster;

import com.yahoo.sdvornik.sharable.Constants;
import com.yahoo.sdvornik.sharable.BroadcastMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToMessageEncoder;


import java.net.InetSocketAddress;
import java.util.List;

public class BroadcastMessageEncoder extends MessageToMessageEncoder<BroadcastMessage> {

    private final InetSocketAddress broadcastAddress;

    public BroadcastMessageEncoder() {
        broadcastAddress = new InetSocketAddress("255.255.255.255", Constants.BROADCAST_PORT);
    }

    @Override
    protected void encode(
            ChannelHandlerContext ctx,
            BroadcastMessage broadcastMessage,
            List<Object> out
    ) throws Exception {
        out.add(new DatagramPacket(broadcastMessage.getByteBuf(), broadcastAddress));
    }
}
