package com.yahoo.sdvornik.message.codec

import com.yahoo.sdvornik.message.DataMessageArray
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToMessageDecoder
import java.util.concurrent.ThreadLocalRandom

/**
 * @author Serg Dvornik <sdvornik@yahoo.com>
 */
class ShuffleDecoder : MessageToMessageDecoder<DataMessageArray>() {

    @Throws(Exception::class)
    override fun decode(ctx: ChannelHandlerContext, msg: DataMessageArray, out: MutableList<Any>) {
        val a = msg.array
        for (i in a.indices.reversed()) {
            val j = random.nextInt(i + 1)
            val tmp = a[j]
            a[j] = a[i]
            a[i] = tmp
        }
        out.add(msg)
    }

    companion object {
        private val random = ThreadLocalRandom.current()
    }
}