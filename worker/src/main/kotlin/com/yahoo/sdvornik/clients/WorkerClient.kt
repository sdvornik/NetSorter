package com.yahoo.sdvornik.clients

import com.yahoo.sdvornik.Constants
import com.yahoo.sdvornik.handlers.WorkerClientHandler
import com.yahoo.sdvornik.message.Message
import com.yahoo.sdvornik.message.codec.BufferToArrayCodec
import com.yahoo.sdvornik.message.codec.ByteBufToMsgDecoder
import com.yahoo.sdvornik.message.codec.MsgToByteEncoder
import com.yahoo.sdvornik.message.codec.ShuffleDecoder
import io.netty.bootstrap.Bootstrap
import io.netty.channel.ChannelInitializer
import io.netty.channel.ChannelOption
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.LengthFieldBasedFrameDecoder
import org.slf4j.LoggerFactory
import java.lang.Long
import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

/**
 * @author Serg Dvornik <sdvornik@yahoo.com>
 */
class WorkerClient
/**
 * Ctor.
 * @param address
 */
(private val address: InetSocketAddress) {

    private val workerGroup = NioEventLoopGroup()

    private val connectionLock = ReentrantLock()

    private val condition = connectionLock.newCondition()

    fun blockingInit(): Boolean {

        val workerBootstrap = Bootstrap()

        workerBootstrap.group(workerGroup)
                .channel(NioSocketChannel::class.java)
                .remoteAddress(address)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(object : ChannelInitializer<SocketChannel>() {
                    @Throws(Exception::class)
                    public override fun initChannel(ch: SocketChannel) {

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
                                ShuffleDecoder(),
                                WorkerClientHandler(connectionLock, condition)
                        )

                    }
                })
        try {
            connectionLock.tryLock(TIMEOUT_IN_MS.toLong(), TimeUnit.MILLISECONDS)
            var future = workerBootstrap.connect()
            val channel = future.sync().channel()
            log.info("Successfully connected to Master node. Try to join into cluster. Send GET_CONNECTION_MESSAGE.")
            future = channel.writeAndFlush(Message.getSimpleOutboundMessage(Message.Type.GET_CONNECTION))
            var time = System.currentTimeMillis()
            condition.await()
            time = System.currentTimeMillis() - time
            log.info("Successfully start WorkerEntryPoint client. Waiting $time ms")
            return true
        } catch (e: InterruptedException) {
            log.info("Can't start WorkerEntryPoint client")
            return false
        } finally {
            connectionLock.unlock()
        }
    }

    fun stop() {
        workerGroup.shutdownGracefully()
    }

    companion object {

        private val log = LoggerFactory.getLogger(WorkerClient::class.java)

        private val TIMEOUT_IN_MS = 1000
    }

}
