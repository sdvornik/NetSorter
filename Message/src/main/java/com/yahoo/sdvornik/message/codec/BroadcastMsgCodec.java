package com.yahoo.sdvornik.message.codec;

import com.yahoo.sdvornik.Constants;
import com.yahoo.sdvornik.message.BroadcastMessage;
import com.yahoo.sdvornik.message.Message;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.ByteToMessageCodec;
import io.netty.handler.codec.MessageToMessageCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;

public class BroadcastMsgCodec extends MessageToMessageCodec<DatagramPacket, BroadcastMessage> {

    private static final Logger log = LoggerFactory.getLogger(BroadcastMsgCodec.class);

    private final InetSocketAddress broadcastAddress;



    public BroadcastMsgCodec() {
        broadcastAddress = new InetSocketAddress("255.255.255.255", Constants.BROADCAST_PORT);
    }
    @Override
    protected void decode(ChannelHandlerContext ctx, DatagramPacket msg, List<Object> out) throws Exception {
        ByteBuf byteBuf = msg.content();
        BroadcastMessage event = BroadcastMessage.getMsgFromBuffer(byteBuf);
        out.add(event);
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, BroadcastMessage msg, List<Object> out) throws Exception {
        out.add(new DatagramPacket(msg.getByteBuf(), broadcastAddress));
    }
}
