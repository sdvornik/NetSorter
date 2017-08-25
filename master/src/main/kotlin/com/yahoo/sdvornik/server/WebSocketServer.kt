package com.yahoo.sdvornik.server

import com.yahoo.sdvornik.Constants
import com.yahoo.sdvornik.handlers.HttpRequestHandler
import com.yahoo.sdvornik.handlers.WebSocketFrameHandler
import com.yahoo.sdvornik.main.MasterEntryPoint
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.*
import io.netty.channel.group.ChannelGroup
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.http.HttpObjectAggregator
import io.netty.handler.codec.http.HttpServerCodec
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler
import io.netty.handler.stream.ChunkedWriteHandler
import org.slf4j.LoggerFactory
import java.net.InetSocketAddress

/**
 * @author Serg Dvornik <sdvornik@yahoo.com>
 */
/**
 * WebSocket server
 */
class WebSocketServer
/**
 * Ctor.
 * @param wsEventLoopGroup
 * @param wsChannelGroup
 */
(private val wsEventLoopGroup: EventLoopGroup, private val wsChannelGroup: ChannelGroup) {

    fun init() {
        val wsBootstrap = ServerBootstrap()
        wsBootstrap.group(wsEventLoopGroup)
                .channel(NioServerSocketChannel::class.java)
                .childHandler(object : ChannelInitializer<Channel>() {
                    @Throws(Exception::class)
                    override fun initChannel(ch: Channel) {
                        ch.pipeline().addLast(
                                HttpServerCodec(),
                                HttpObjectAggregator(64 * 1024),
                                ChunkedWriteHandler(),
                                HttpRequestHandler("/ws"),
                                WebSocketServerProtocolHandler("/ws"),
                                WebSocketFrameHandler()
                        )
                    }
                })

        val wsFuture = wsBootstrap.bind(InetSocketAddress(Constants.WEBSOCKET_PORT))

        wsFuture.addListener(ChannelFutureListener { future ->
            if (!future.isSuccess) {
                log.error("Can't run WebSocket server", future.cause())
                MasterEntryPoint.INSTANCE.stop()
                return@ChannelFutureListener
            }
            log.info("Successfully init WebSocket server")
        })
    }

    companion object {
        private val log = LoggerFactory.getLogger(WebSocketServer::class.java)
    }
}