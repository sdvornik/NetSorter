package com.yahoo.sdvornik.message.codec

import com.yahoo.sdvornik.message.DataMessageArray
import com.yahoo.sdvornik.message.DataMessageBuffer
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToMessageCodec
import org.slf4j.LoggerFactory

/**
 * @author Serg Dvornik <sdvornik@yahoo.com>
 */
class BufferToArrayCodec : MessageToMessageCodec<DataMessageBuffer, DataMessageArray>() {

    @Throws(Exception::class)
    override fun encode(ctx: ChannelHandlerContext, msg: DataMessageArray, out: MutableList<Any>) {
        val arr = msg.array

        val buf = Unpooled.buffer(arr.size * java.lang.Long.BYTES)
        for (i in arr.indices) {
            buf.writeLong(arr[i])
        }
        val dataMsgBuf = DataMessageBuffer(buf, msg.chunkNumber)
        out.add(dataMsgBuf)
    }

    @Throws(Exception::class)
    override fun decode(ctx: ChannelHandlerContext, msg: DataMessageBuffer, out: MutableList<Any>) {

        val buf = msg.nettyBuf
        val length = buf.readableBytes() / java.lang.Long.BYTES
        val arr = LongArray(length)

        for (i in 0..length - 1) {
            arr[i] = buf.readLong()
        }
        buf.release()

        out.add(DataMessageArray(arr, msg.chunkNumber))
    }
}