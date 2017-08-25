package com.yahoo.sdvornik.message.codec

import com.yahoo.sdvornik.message.DataMessageBuffer
import com.yahoo.sdvornik.message.Message
import com.yahoo.sdvornik.message.StartSortingMessage
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToMessageDecoder

/**
 * @author Serg Dvornik <sdvornik@yahoo.com>
 */
class ByteBufToMsgDecoder : MessageToMessageDecoder<ByteBuf>() {

    @Throws(Exception::class)
    override fun decode(ctx: ChannelHandlerContext, `in`: ByteBuf, out: MutableList<Any>) {

        val typeInt = `in`.readInt()

        val type = Message.toEnum(typeInt)

        when (type) {
            Message.Type.GET_CONNECTION,
            Message.Type.JOB_ENDED,
            Message.Type.TASK_TRANSMISSION_ENDED,
            Message.Type.GET_RESULT,
            Message.Type.CONNECTED -> out.add(Message.getSimpleOutboundMessage(type))

            Message.Type.DATA_TYPE -> {
                val chunkNumber = `in`.readInt()
                out.add(DataMessageBuffer(`in`, chunkNumber))
                `in`.retain()
            }

            Message.Type.START_SORTING -> {
                val taskCounter = `in`.readInt()
                out.add(StartSortingMessage(taskCounter))
            }
        }
    }
}