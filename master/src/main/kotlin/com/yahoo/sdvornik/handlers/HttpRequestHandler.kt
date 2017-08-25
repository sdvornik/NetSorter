package com.yahoo.sdvornik.handlers

import io.netty.channel.ChannelHandler
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.DefaultFileRegion
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.handler.codec.http.*
import org.slf4j.LoggerFactory
import java.io.File
import java.io.RandomAccessFile
import java.nio.file.Paths

/**
 * @author Serg Dvornik <sdvornik@yahoo.com>
 */

@ChannelHandler.Sharable
class HttpRequestHandler(private val uri: String) : SimpleChannelInboundHandler<FullHttpRequest>() {

    @Throws(Exception::class)
    public override fun channelRead0(ctx: ChannelHandlerContext, request: FullHttpRequest) {
        if (uri.equals(request.uri(), ignoreCase = true)) {
            log.info("Create WebSocket connection")
            ctx.fireChannelRead(request.retain())
        }
        else {
            log.info("Send index.html")

            val file = RandomAccessFile(INDEX_FILE, "r")

            val response = DefaultHttpResponse(request.protocolVersion(), HttpResponseStatus.OK)

            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/html; charset=UTF-8")
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, file.length())
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE)

            ctx.write(response)
            ctx.write(DefaultFileRegion(file.channel, 0, file.length()))

            ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT)
        }
    }


    companion object {

        private val INDEX_FILE_NAME = "index.html"

        private val INDEX_FILE: File

        private val log = LoggerFactory.getLogger(HttpRequestHandler::class.java)

        init {
            val classLoader = Thread.currentThread().contextClassLoader
            try {
                val PATH_TO_INDEX_FILE = Paths.get(classLoader.getResource(INDEX_FILE_NAME)!!.toURI())
                INDEX_FILE = File(PATH_TO_INDEX_FILE.toString())
            } catch (e: Exception) {
                throw RuntimeException("Can't find index.html", e)
            }

        }
    }
}
