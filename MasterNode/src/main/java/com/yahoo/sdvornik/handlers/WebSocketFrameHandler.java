package com.yahoo.sdvornik.handlers;

import com.yahoo.sdvornik.Constants;
import com.yahoo.sdvornik.main.MasterEntryPoint;
import com.yahoo.sdvornik.utils.KeyGenerator;
import com.yahoo.sdvornik.master.MasterTask;
import fj.Try;
import fj.data.Validation;
import io.netty.channel.*;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;

public class WebSocketFrameHandler extends SimpleChannelInboundHandler<TextWebSocketFrame> {


    private static final Logger log = LoggerFactory.getLogger(WebSocketFrameHandler.class);

    private static final String JSON_TYPE_FIELD_NAME = "type";

    private static final String JSON_CONTENT_FIELD_NAME = "content";

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {

        if (evt.getClass() == WebSocketServerProtocolHandler.HandshakeComplete.class) {
            ctx.pipeline().remove(HttpRequestHandler.class);
            final Channel wsChannel = ctx.channel();
            MasterEntryPoint.INSTANCE.addChannelToWebSocketGroup(ctx.channel());

            ctx.writeAndFlush(new TextWebSocketFrame("Successfully connected to MasterEntryPoint node"));
            log.info("WebSocket client connected. ID: " + ctx.channel().id().asShortText());

            ChannelFuture closeFuture = ctx.channel().closeFuture();

            closeFuture.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    log.info("WebSocket client disconnected. ID: " + ctx.channel().id().asShortText());
                    MasterEntryPoint.INSTANCE.removeChannelFromWebSocketGroup(ctx.channel());
                }
            });
        }
        else {
            super.userEventTriggered(ctx, evt);
        }
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, TextWebSocketFrame msg) throws Exception {

        JSONObject json = new JSONObject(msg.text());
        final String type = (String)json.get("type");
        final String content = (String) json.get("content");

        ctx.writeAndFlush(new TextWebSocketFrame("Command "+type+" accepted by master node"));

        switch(type) {
            case("GENERATE") :
                final Integer size_in_mbytes = Try.f(
                        (String str) -> Integer.valueOf(str)
                ).f(content).toOption().toNull();
                log.info("Size: "+size_in_mbytes);
                ctx.executor().execute(new Runnable(){
                    @Override
                    public void run() {
                        ctx.executor().execute(
                                new Runnable() {

                                    @Override
                                    public void run() {
                                        Validation<? extends Exception, Path> val =
                                                KeyGenerator.INSTANCE.generateFile(size_in_mbytes);
                                        String msg = val.isFail() ?
                                            "Can't generate file:"+val.fail().getMessage() :
                                            "File successfully created. Path to file: " + val.toOption().toNull() +
                                                ". Size: "+size_in_mbytes+" MB.";
                                        ctx.writeAndFlush(new TextWebSocketFrame(msg));
                                        log.info(msg);
                                    }
                                }
                        );
                    }
                });
                break;
            case("SHUTDOWN"):
                MasterEntryPoint.INSTANCE.stop();
                break;

            case("RUN"):

                final Path pathToFile = (content == null) ?
                        Paths.get(System.getProperty("user.home"), Constants.DEFAULT_FILE_NAME) :
                        Paths.get(content);
                log.info(pathToFile.toString());
                ctx.executor().execute(new Runnable(){
                    @Override
                    public void run() {
                        ctx.executor().execute(
                                new Runnable() {

                                    @Override
                                    public void run() {
                                        try {
                                            MasterTask.INSTANCE.runTask(pathToFile, ctx.channel());
                                        }
                                        catch(Exception e) {
                                            String msg = "Can't distribute task: "+e.getMessage();
                                            ctx.writeAndFlush(new TextWebSocketFrame(msg));
                                            log.info(msg);
                                        }
                                    }
                                }
                        );
                    }
                });
                break;
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) throws Exception {
        log.error("Exception in WebSocketFrameHandler", e);
        ctx.close();
    }

}
