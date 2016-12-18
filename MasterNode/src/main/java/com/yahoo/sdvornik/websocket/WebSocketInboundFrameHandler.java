package com.yahoo.sdvornik.websocket;

import com.yahoo.sdvornik.Constants;
import com.yahoo.sdvornik.generator.KeyGenerator;
import com.yahoo.sdvornik.main.EntryPoint;
import com.yahoo.sdvornik.master.MasterTaskSender;
import com.yahoo.sdvornik.server.WebSocketServer;
import fj.Try;
import fj.Unit;
import fj.data.Validation;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;

public class WebSocketInboundFrameHandler extends SimpleChannelInboundHandler<TextWebSocketFrame> {


    private static final Logger log = LoggerFactory.getLogger(WebSocketInboundFrameHandler.class.getName());

    private static final String JSON_TYPE_FIELD_NAME = "type";

    private static final String JSON_CONTENT_FIELD_NAME = "content";

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {

        if (evt.getClass() == WebSocketServerProtocolHandler.HandshakeComplete.class) {
            ctx.pipeline().remove(HttpRequestHandler.class);
            final Channel wsChannel = ctx.channel();
            EntryPoint.addChannelToWebSocketGroup(ctx.channel());

            ctx.writeAndFlush(new TextWebSocketFrame("Successfully connected to Master node"));
            log.info("WebSocket client connected. ID: " + ctx.channel().id().asShortText());

            ChannelFuture closeFuture = ctx.channel().closeFuture();

            closeFuture.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    log.info("WebSocket client disconnected. ID: " + ctx.channel().id().asShortText());
                    EntryPoint.removeChannelFromWebSocketGroup(ctx.channel());
                }
            });
        }
        else {
            super.userEventTriggered(ctx, evt);
        }
    }

    //TODO remove
    /*
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        EntryPoint.removeChannelFromWebSocketGroup(ctx.channel());
        log.info("WebSocket client disconnected. ID: " + ctx.channel().id());
    }
*/


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

                ctx.executor().execute(new Runnable(){
                    @Override
                    public void run() {
                        ctx.executor().execute(
                                new Runnable() {

                                    @Override
                                    public void run() {
                                        Validation<? extends Exception, Path> val =
                                                KeyGenerator.INSTANCE.generateFile(size_in_mbytes);
                                        String msg = null;
                                        if(val.isFail()) {
                                            msg = "Can't generate file:"+val.fail().getMessage();
                                        }
                                        else {
                                            msg = "File successfully created. Path to file: " + val.toOption().toNull();
                                        }
                                        ctx.writeAndFlush(new TextWebSocketFrame(msg));
                                        log.info(msg);
                                    }
                                }
                        );
                    }
                });
                break;
            case("SHUTDOWN"):
                EntryPoint.stop();
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
                                        Validation<? extends Exception, Unit> val =
                                               new MasterTaskSender(pathToFile).distributeTask();
                                        String msg = null;
                                        if(val.isFail()) {
                                            msg = "Can't distribute task: "+val.fail().getMessage();
                                        }
                                        else {
                                            msg = "Task successfully distributed";
                                        }
                                        ctx.writeAndFlush(new TextWebSocketFrame(msg));
                                        log.info(msg);
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
        log.error("Exception in WebSocketInboundFrameHandler", e);
        ctx.close();
    }

}
