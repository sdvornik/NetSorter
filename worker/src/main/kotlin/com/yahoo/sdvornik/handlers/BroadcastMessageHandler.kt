package com.yahoo.sdvornik.handlers

import com.yahoo.sdvornik.clients.WorkerClient
import com.yahoo.sdvornik.main.WorkerEntryPoint
import com.yahoo.sdvornik.message.BroadcastMessage
import io.netty.channel.ChannelFutureListener
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import org.slf4j.LoggerFactory
import java.net.InetSocketAddress

/**
 * @author Serg Dvornik <sdvornik@yahoo.com>
 */
class BroadcastMessageHandler : SimpleChannelInboundHandler<BroadcastMessage>() {


    @Throws(Exception::class)
    public override fun channelRead0(ctx: ChannelHandlerContext, msg: BroadcastMessage) {
        log.info(
                "Received message. Address: " + msg.serverAddress.hostAddress +
                        "; Port: " + msg.serverPort
        )

        ctx.close().addListener(ChannelFutureListener { future ->
            if (!future.isSuccess) {
                log.error("Can't close Broadcast listener", future.cause())
                return@ChannelFutureListener
            }
            log.info("Successfully close Broadcast listener")
        })

        val address = InetSocketAddress(msg.serverAddress, msg.serverPort)
        ctx.executor().execute {
            if (WorkerClient(address).blockingInit()) {
                WorkerEntryPoint.INSTANCE.stopBroadcastListener()
            }
        }
    }


    companion object {
        private val log = LoggerFactory.getLogger(BroadcastMessageHandler::class.java!!)
    }
}