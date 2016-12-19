package com.yahoo.sdvornik.broadcastlistener;

import com.yahoo.sdvornik.sharable.BroadcastMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.util.List;

public class BroadcastMessageDecoder extends MessageToMessageDecoder<DatagramPacket> {
    @Override
    protected void decode(ChannelHandlerContext ctx, DatagramPacket datagramPacket, List<Object> out) throws Exception {
        ByteBuf byteBuf = datagramPacket.content();
        BroadcastMessage event = BroadcastMessage.getMsgFromBuffer(byteBuf);
        out.add(event);
    }
}

