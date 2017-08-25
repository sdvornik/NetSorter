package com.yahoo.sdvornik.message.codec

import com.yahoo.sdvornik.message.Message
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToByteEncoder
/**
 * @author Serg Dvornik <sdvornik@yahoo.com>
 */
class MsgToByteEncoder : MessageToByteEncoder<Message>() {

    @Throws(Exception::class)
    override fun encode(ctx: ChannelHandlerContext, msg: Message, out: ByteBuf) {
        msg.writeHeader(out)
        when (msg.type) {
            Message.Type.DATA_TYPE, Message.Type.START_SORTING -> msg.writeContent(out)
            else -> throw IllegalArgumentException("Out of range")
        }
    }
}