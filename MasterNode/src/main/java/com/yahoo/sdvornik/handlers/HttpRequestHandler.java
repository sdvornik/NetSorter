package com.yahoo.sdvornik.handlers;

import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.nio.file.Paths;

@ChannelHandler.Sharable
public class HttpRequestHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private static final String INDEX_FILE_NAME = "index.html";

    private static final File INDEX_FILE;

    private static final Logger log = LoggerFactory.getLogger(HttpRequestHandler.class.getName());

    static {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        try {
            Path PATH_TO_INDEX_FILE = Paths.get(classLoader.getResource(INDEX_FILE_NAME).toURI());
            INDEX_FILE = new File(PATH_TO_INDEX_FILE.toString());
        } catch (Exception e) {
            throw new RuntimeException("Can't find index.html", e);
        }
    }

    private final String uri;

    public HttpRequestHandler(String uri) {
        this.uri = uri;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
        if (uri.equalsIgnoreCase(request.uri())) {
            log.info("Create WebSocket connection");
            ctx.fireChannelRead(request.retain());
        }
        else {
            log.info("Send index.html");

            RandomAccessFile file = new RandomAccessFile(INDEX_FILE, "r");

            HttpResponse response = new DefaultHttpResponse(request.protocolVersion(), HttpResponseStatus.OK);

            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/html; charset=UTF-8");
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, file.length());
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);

            ctx.write(response);
            ctx.write(new DefaultFileRegion(file.getChannel(), 0, file.length()));

            ChannelFuture future = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error(cause.getMessage());
        ctx.close();
    }
}
