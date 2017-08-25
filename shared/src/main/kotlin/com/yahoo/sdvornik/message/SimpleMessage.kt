package com.yahoo.sdvornik.message

import io.netty.buffer.ByteBuf

/**
 * @author Serg Dvornik <sdvornik@yahoo.com>
 */
class SimpleMessage (override val type: Message.Type) : Message() {

    override val length: Long
        get() = Integer.BYTES.toLong()

    override fun writeContent(buf: ByteBuf) {
        throw UnsupportedOperationException("Can't write content for SimpleMessage")
    }
}