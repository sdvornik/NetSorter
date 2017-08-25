package com.yahoo.sdvornik.message

import io.netty.buffer.ByteBuf

/**
 * @author Serg Dvornik <sdvornik@yahoo.com>
 */
class StartSortingMessage(val content: Int) : Message() {

    override val type: Message.Type = Message.Type.START_SORTING

    override val length: Long= 2 * Integer.BYTES.toLong()

    override fun writeContent(buf: ByteBuf) {
        buf.writeInt(content)
    }
}