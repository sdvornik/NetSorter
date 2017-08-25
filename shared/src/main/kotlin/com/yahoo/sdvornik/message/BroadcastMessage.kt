package com.yahoo.sdvornik.message

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import org.slf4j.LoggerFactory
import java.net.InetAddress
import java.net.UnknownHostException

/**
 * @author Serg Dvornik <sdvornik@yahoo.com>
 */
/**
 * Immutable class which represents master node connection data.
 */
class BroadcastMessage
/**
 * Ctor.
 * @param serverAddress
 * @param serverPort
 */
(val serverAddress: InetAddress, val serverPort: Int) {

    val byteBuf: ByteBuf
        get() {
            val buffer = Unpooled.buffer(2 * Integer.BYTES)
            buffer.writeBytes(serverAddress.address)
            buffer.writeInt(serverPort)
            return buffer
        }

    companion object {

        private val log = LoggerFactory.getLogger(BroadcastMessage::class.java)

        @JvmStatic fun getMsgFromBuffer(buffer: ByteBuf): BroadcastMessage? {

            val byteArr = ByteArray(Integer.BYTES)
            buffer.readBytes(byteArr)

            try {
                val serverAddress = InetAddress.getByAddress(byteArr)
                val serverPort = buffer.readInt()
                return BroadcastMessage(serverAddress, serverPort)
            } catch (e: UnknownHostException) {
                log.error("Can't create BroadcastMessage", e)
                return null
            }

        }
    }

}