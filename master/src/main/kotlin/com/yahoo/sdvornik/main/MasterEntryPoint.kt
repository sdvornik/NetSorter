package com.yahoo.sdvornik.main

import com.yahoo.sdvornik.server.BroadcastServer
import com.yahoo.sdvornik.server.MasterServer
import com.yahoo.sdvornik.server.WebSocketServer
import io.netty.channel.Channel
import io.netty.channel.group.ChannelGroup
import io.netty.channel.group.DefaultChannelGroup
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame
import io.netty.util.concurrent.GlobalEventExecutor
import org.slf4j.LoggerFactory

/**
 * @author Serg Dvornik <sdvornik@yahoo.com>
 */
/**
 * Main class for master node
 */
enum class MasterEntryPoint {

    INSTANCE;

    private val log = LoggerFactory.getLogger(MasterEntryPoint::class.java)

    private val masterEventLoopGroup = NioEventLoopGroup()

    private val wsEventLoopGroup = NioEventLoopGroup()

    private val udpEventLoopGroup = NioEventLoopGroup()

    private val wsChannelGroup = DefaultChannelGroup(GlobalEventExecutor.INSTANCE)

    /**
     * Getter for group of worker nodes.
     * @return
     */
    val masterChannelGroup: ChannelGroup = DefaultChannelGroup(GlobalEventExecutor.INSTANCE)

    /**
     * Send message to all WebSocket connections
     * @param msg
     */
    fun sendMsgToWebSocketGroup(msg: String) {
        wsChannelGroup.writeAndFlush(TextWebSocketFrame(msg))
    }

    /**
     * Add [Channel] channel of worker node to group of worker node.
     * @param channel
     */
    fun addChannelToMasterGroup(channel: Channel) {
        masterChannelGroup.add(channel)
    }

    /**
     * Remove [Channel] channel of worker from group.
     * @param channel
     */
    fun removeChannelFromMasterGroup(channel: Channel) {
        masterChannelGroup.remove(channel)
    }

    /**
     * Add [Channel] WebSocket channel to group of WebSocket connections.
     * @param channel
     */
    fun addChannelToWebSocketGroup(channel: Channel) {
        wsChannelGroup.add(channel)
    }

    /**
     * Remove [Channel] WebSocket channel from group of WebSocket connections.
     * @param channel
     */
    fun removeChannelFromWebSocketGroup(channel: Channel) {
        wsChannelGroup.remove(channel)
    }

    /**
     * Shutdown master node.
     */
    fun stop() {
        val closeWsGroupFuture = wsChannelGroup.close()
        closeWsGroupFuture.addListener {
            wsEventLoopGroup.shutdownGracefully()
            log.info("Successfully shutdown WebSocket server")
        }

        val closeMasterGroupFuture = masterChannelGroup.close()
        closeMasterGroupFuture.addListener {
            INSTANCE.masterEventLoopGroup.shutdownGracefully()
            INSTANCE.log.info("Successfully shutdown MasterEntryPoint server")
        }

        val shutdownBroadcasterEventLoop = udpEventLoopGroup.shutdownGracefully()
        shutdownBroadcasterEventLoop.awaitUninterruptibly()
        log.info("Successfully shutdown Broadcast server")
    }

    companion object {

        @Throws(Exception::class)
        @JvmStatic
        fun main(args: Array<String>) {

            WebSocketServer(
                    INSTANCE.wsEventLoopGroup,
                    INSTANCE.wsChannelGroup
            ).init()

            BroadcastServer(
                    INSTANCE.udpEventLoopGroup
            ).init()

            MasterServer(
                    INSTANCE.masterEventLoopGroup,
                    INSTANCE.masterChannelGroup
            ).init()
        }
    }
}
