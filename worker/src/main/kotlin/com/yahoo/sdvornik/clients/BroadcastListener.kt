package com.yahoo.sdvornik.clients

import com.yahoo.sdvornik.Constants
import com.yahoo.sdvornik.handlers.BroadcastMessageHandler
import com.yahoo.sdvornik.message.codec.BroadcastMsgCodec
import io.netty.bootstrap.Bootstrap
import io.netty.channel.Channel
import io.netty.channel.ChannelInitializer
import io.netty.channel.ChannelOption
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioDatagramChannel
import org.slf4j.LoggerFactory
import java.net.InetSocketAddress

/**
 * @author Serg Dvornik <sdvornik@yahoo.com>
 */
class BroadcastListener {

    private val broadcastListenerGroup = NioEventLoopGroup()

    /**
     * Blocking initialization of instance
     * @return
     */
    fun blockingInit(): Boolean {
        val bootstrap = Bootstrap()
        val address = InetSocketAddress(Constants.BROADCAST_PORT)
        bootstrap.group(broadcastListenerGroup)
                .channel(NioDatagramChannel::class.java)
                .option(ChannelOption.SO_BROADCAST, true)
                .handler(object : ChannelInitializer<Channel>() {
                    @Throws(Exception::class)
                    public override fun initChannel(ch: Channel) {
                        ch.pipeline().addLast(
                                BroadcastMsgCodec(),
                                BroadcastMessageHandler()
                        )
                    }
                })
                .localAddress(address)
        try {
            bootstrap.bind().sync()
            log.info("BroadcastListener successfully started")
        } catch (e: InterruptedException) {
            log.error("Can't start BroadcastListener")
            return false
        }

        return true
    }

    fun stop() {
        broadcastListenerGroup.shutdownGracefully()
    }

    companion object {

        private val log = LoggerFactory.getLogger(BroadcastListener::class.java)
    }

}
