package com.yahoo.sdvornik.handlers

import com.yahoo.sdvornik.Constants
import com.yahoo.sdvornik.main.MasterEntryPoint
import com.yahoo.sdvornik.master.MasterTask
import com.yahoo.sdvornik.utils.KeyGenerator
import fj.Try
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler
import org.json.JSONObject
import org.slf4j.LoggerFactory
import java.nio.file.Paths

/**
 * @author Serg Dvornik <sdvornik@yahoo.com>
 */
class WebSocketFrameHandler : SimpleChannelInboundHandler<TextWebSocketFrame>() {

    @Throws(Exception::class)
    override fun userEventTriggered(ctx: ChannelHandlerContext, evt: Any) {

        if (evt.javaClass == WebSocketServerProtocolHandler.HandshakeComplete::class.java) {
            ctx.pipeline().remove(HttpRequestHandler::class.java)
            val wsChannel = ctx.channel()
            MasterEntryPoint.INSTANCE.addChannelToWebSocketGroup(ctx.channel())

            ctx.writeAndFlush(TextWebSocketFrame("Successfully connected to MasterEntryPoint node"))
            log.info("WebSocket client connected. ID: " + ctx.channel().id().asShortText())

            val closeFuture = ctx.channel().closeFuture()

            closeFuture.addListener {
                log.info("WebSocket client disconnected. ID: " + ctx.channel().id().asShortText())
                MasterEntryPoint.INSTANCE.removeChannelFromWebSocketGroup(ctx.channel())
            }
        } else {
            super.userEventTriggered(ctx, evt)
        }
    }

    @Throws(Exception::class)
    public override fun channelRead0(ctx: ChannelHandlerContext, msg: TextWebSocketFrame) {

        val json = JSONObject(msg.text())
        val type = json.get("type") as String
        val content = json.get("content") as String

        ctx.writeAndFlush(TextWebSocketFrame("Command $type accepted by master node"))

        when (type) {
            ("GENERATE") -> {
                val size_in_mbytes = Try.f<String, Int, RuntimeException> { str: String -> Integer.valueOf(str) }.f(content).toOption().toNull()
                log.info("Size: " + size_in_mbytes!!)
                ctx.executor().execute(object : Runnable {
                    override fun run() {
                        ctx.executor().execute {
                            val `val` = KeyGenerator.INSTANCE.generateFile(size_in_mbytes)
                            val msg = if (`val`.isFail)
                                "Can't generate file:" + `val`.fail().message
                            else
                                "File successfully created. Path to file: " + `val`.toOption().toNull() +
                                        ". Size: " + size_in_mbytes + " MB."
                            ctx.writeAndFlush(TextWebSocketFrame(msg))
                            log.info(msg)
                        }
                    }
                })
            }
            ("SHUTDOWN") -> MasterEntryPoint.INSTANCE.stop()

            ("RUN") -> {

                val pathToFile = if ((content == null))
                    Paths.get(System.getProperty("user.home"), Constants.DEFAULT_FILE_NAME)
                else
                    Paths.get(content)
                log.info(pathToFile.toString())
                ctx.executor().execute(object : Runnable {
                    override fun run() {
                        ctx.executor().execute(
                                object : Runnable {

                                    override fun run() {
                                        try {
                                            MasterTask.INSTANCE.runTask(pathToFile, ctx.channel())
                                        } catch (e: Exception) {
                                            val msg = "Can't distribute task: " + e.message
                                            ctx.writeAndFlush(TextWebSocketFrame(msg))
                                            log.info(msg)
                                        }

                                    }
                                }
                        )
                    }
                })
            }
        }
    }

    @Throws(Exception::class)
    override fun exceptionCaught(ctx: ChannelHandlerContext, e: Throwable) {
        log.error("Exception in WebSocketFrameHandler", e)
        ctx.close()
    }

    companion object {


        private val log = LoggerFactory.getLogger(WebSocketFrameHandler::class.java!!)

        private val JSON_TYPE_FIELD_NAME = "type"

        private val JSON_CONTENT_FIELD_NAME = "content"
    }

}
