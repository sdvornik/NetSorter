package com.yahoo.sdvornik.server

import com.yahoo.sdvornik.Constants
import com.yahoo.sdvornik.main.MasterEntryPoint
import com.yahoo.sdvornik.message.BroadcastMessage
import com.yahoo.sdvornik.message.codec.BroadcastMsgCodec
import com.yahoo.sdvornik.utils.Utils
import io.netty.bootstrap.Bootstrap
import io.netty.channel.ChannelFuture
import io.netty.channel.ChannelFutureListener
import io.netty.channel.ChannelOption
import io.netty.channel.EventLoopGroup
import io.netty.channel.socket.nio.NioDatagramChannel
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

/**
 * @author Serg Dvornik <sdvornik@yahoo.com>
 */
/**
 * UDP broadcasting server
 */
class BroadcastServer
/**
 * Ctor.
 */
(private val udpEventLoopGroup: EventLoopGroup) {


    fun init() {
        val udpBootstrap = Bootstrap()

        udpBootstrap.group(udpEventLoopGroup)
                .channel(NioDatagramChannel::class.java)
                .option(ChannelOption.SO_BROADCAST, true)
                .handler(BroadcastMsgCodec())


        val udpFuture = udpBootstrap.bind(0)
        udpFuture.addListener(ChannelFutureListener { future ->
            if (!future.isSuccess) {
                log.error("Can't run Broadcast server", future.cause())
                MasterEntryPoint.INSTANCE.stop()
                return@ChannelFutureListener
            }
            log.info("Successfully init Broadcast server")
            val udpChannel = future.channel()
            udpEventLoopGroup.scheduleAtFixedRate(
                    object : Runnable {
                        internal val broadcastMessage = BroadcastMessage(Utils.getLocalHostAddress(), Constants.PORT)
                        override fun run() {
                            udpChannel.writeAndFlush(broadcastMessage)
                        }
                    },
                    0,
                    Constants.BROADCAST_INTERVAL_IN_MS.toLong(),
                    TimeUnit.MILLISECONDS
            )
        })
    }

    companion object {

        private val log = LoggerFactory.getLogger(BroadcastServer::class.java)
    }
}