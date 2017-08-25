package com.yahoo.sdvornik.server

import com.yahoo.sdvornik.Constants
import com.yahoo.sdvornik.handlers.MasterServerHandler
import com.yahoo.sdvornik.main.MasterEntryPoint
import com.yahoo.sdvornik.message.codec.BufferToArrayCodec
import com.yahoo.sdvornik.message.codec.ByteBufToMsgDecoder
import com.yahoo.sdvornik.message.codec.MsgToByteEncoder
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.*
import io.netty.channel.group.ChannelGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.LengthFieldBasedFrameDecoder
import org.slf4j.LoggerFactory
import java.lang.Long
import java.net.InetSocketAddress

/**
 * @author Serg Dvornik <sdvornik@yahoo.com>
 */
/**
 * MasterEntryPoint server
 */
class MasterServer
/**
 * Ctor.
 * @param masterEventLoopGroup
 * @param masterChannelGroup
 */
(private val masterEventLoopGroup: EventLoopGroup, private val masterChannelGroup: ChannelGroup) {

    @Throws(Exception::class)
    fun init() {
        val masterBootstrap = ServerBootstrap()
        masterBootstrap.group(masterEventLoopGroup)
                .channel(NioServerSocketChannel::class.java)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .localAddress(InetSocketAddress(Constants.PORT))
                .childHandler(object : ChannelInitializer<SocketChannel>() {

                    @Throws(Exception::class)
                    override fun initChannel(ch: SocketChannel) {

                        ch.pipeline().addLast(
                                LengthFieldBasedFrameDecoder(
                                        2 * Constants.DEFAULT_CHUNK_SIZE_IN_KEYS * Long.BYTES,
                                        0,
                                        Long.BYTES,
                                        0,
                                        Long.BYTES
                                ),
                                ByteBufToMsgDecoder(),
                                MsgToByteEncoder(),
                                BufferToArrayCodec(),
                                MasterServerHandler()
                        )
                    }
                })

        val masterFuture = masterBootstrap.bind()

        masterFuture.addListener(ChannelFutureListener { future ->
            if (!future.isSuccess) {
                log.error("Can't run MasterEntryPoint server", future.cause())
                MasterEntryPoint.INSTANCE.stop()
                return@ChannelFutureListener
            }

            log.info("Successfully init MasterEntryPoint server")
        })

    }

    companion object {

        private val log = LoggerFactory.getLogger(MasterServer::class.java)
    }
}
