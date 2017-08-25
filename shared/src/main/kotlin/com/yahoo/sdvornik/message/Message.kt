package com.yahoo.sdvornik.message

import io.netty.buffer.ByteBuf

/**
 * @author Serg Dvornik <sdvornik@yahoo.com>
 */
abstract class Message {

    abstract val type: Type

    abstract val length: Long

    fun writeHeader(buf: ByteBuf) {
        buf.writeLong(length)
        buf.writeInt(type.value)
    }

    abstract fun writeContent(buf: ByteBuf)

    enum class Type private constructor(val value: Int) {
        DATA_TYPE(0),
        GET_CONNECTION(-1),
        CONNECTED(-2),
        START_SORTING(-3),
        TASK_TRANSMISSION_ENDED(-4),
        JOB_ENDED(-5),
        GET_RESULT(-6)

    }

    companion object {

        @JvmStatic fun getSimpleOutboundMessage(type: Type): SimpleMessage {
            return SimpleMessage(type)
        }

        @JvmStatic fun toEnum(i: Int): Type {
            var res: Type? = null
            when (i) {
                0 -> res = Type.DATA_TYPE
                -1 -> res = Type.GET_CONNECTION
                -2 -> res = Type.CONNECTED
                -3 -> res = Type.START_SORTING
                -4 -> res = Type.TASK_TRANSMISSION_ENDED
                -5 -> res = Type.JOB_ENDED
                -6 -> res = Type.GET_RESULT
                else -> throw IllegalArgumentException()
            }
            return res
        }
    }
}
