package com.yahoo.sdvornik.message

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import java.nio.ByteBuffer

/**
 * @author Serg Dvornik <sdvornik@yahoo.com>
 */
class DataMessageBuffer(val nettyBuf: ByteBuf, val chunkNumber: Int) : Message() {

    constructor(nioBuf: ByteBuffer, chunkNumber: Int) : this(Unpooled.wrappedBuffer(nioBuf), chunkNumber)

    override val type = Message.Type.DATA_TYPE

    override val length: Long = (2 * Integer.BYTES + nettyBuf.readableBytes()).toLong()

    override fun writeContent(buf: ByteBuf) {
        buf.writeInt(chunkNumber)
        buf.writeBytes(nettyBuf)
    }

}
