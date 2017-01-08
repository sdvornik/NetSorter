package com.yahoo.sdvornik.clients;

import com.yahoo.sdvornik.Constants;
import com.yahoo.sdvornik.handlers.BroadcastMessageHandler;
import com.yahoo.sdvornik.message.codec.BroadcastMsgCodec;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * Instance of this class listens UDP broadcasting
 * to receive IP-address and port from Master node
 */
public class BroadcastListener {

    private static final Logger log = LoggerFactory.getLogger(BroadcastListener.class.getName());

    private final EventLoopGroup broadcastListenerGroup = new NioEventLoopGroup();

    /**
     * Blocking initialization of instance
     * @return
     */
    public boolean blockingInit() {
        final Bootstrap bootstrap = new Bootstrap();
        final InetSocketAddress address = new InetSocketAddress(Constants.BROADCAST_PORT);
        bootstrap.group(broadcastListenerGroup)
                .channel(NioDatagramChannel.class)
                .option(ChannelOption.SO_BROADCAST, true)
                .handler(new ChannelInitializer<Channel>() {
                    @Override
                    public void initChannel(Channel ch) throws Exception {
                        ch.pipeline().addLast(
                                new BroadcastMsgCodec(),
                                new BroadcastMessageHandler()
                        );
                    }
                })
                .localAddress(address);
        try {
            bootstrap.bind().sync();
            log.info("BroadcastListener successfully started");
        }
        catch(InterruptedException e) {
            log.error("Can't start BroadcastListener");
            return false;
        }
        return true;
    }

    public void stop() {
        broadcastListenerGroup.shutdownGracefully();
    }

}
