package com.yahoo.sdvornik.message.codec

import com.yahoo.sdvornik.Constants
import com.yahoo.sdvornik.message.BroadcastMessage
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.socket.DatagramPacket
import io.netty.handler.codec.MessageToMessageCodec
import java.net.InetSocketAddress

/**
 * @author Serg Dvornik <sdvornik@yahoo.com>
 */
class BroadcastMsgCodec : MessageToMessageCodec<DatagramPacket, BroadcastMessage>() {

    private val broadcastAddress: InetSocketAddress

    init {
        broadcastAddress = InetSocketAddress("255.255.255.255", Constants.BROADCAST_PORT)
    }

    @Throws(Exception::class)
    override fun decode(ctx: ChannelHandlerContext, msg: DatagramPacket, out: MutableList<Any>) {
        val byteBuf = msg.content()
        val event = BroadcastMessage.getMsgFromBuffer(byteBuf) as Any
        out.add(event)
    }

    @Throws(Exception::class)
    override fun encode(ctx: ChannelHandlerContext, msg: BroadcastMessage, out: MutableList<Any>) {
        out.add(DatagramPacket(msg.byteBuf, broadcastAddress))
    }

}