package com.yahoo.sdvornik.clients;

import com.yahoo.sdvornik.message.Message;
import com.yahoo.sdvornik.message.codec.BufferToArrayCodec;
import com.yahoo.sdvornik.message.codec.ByteBufToMsgDecoder;
import com.yahoo.sdvornik.Constants;
import com.yahoo.sdvornik.handlers.WorkerClientHandler;
import com.yahoo.sdvornik.message.codec.MsgToByteEncoder;
import com.yahoo.sdvornik.message.codec.ShuffleDecoder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Instance of this class holds connection to master node.
 */
public class WorkerClient {

    private static final Logger log = LoggerFactory.getLogger(WorkerClient.class);

    private static final int TIMEOUT_IN_MS = 1000;

    private final InetSocketAddress address;

    private final EventLoopGroup workerGroup = new NioEventLoopGroup();

    private final ReentrantLock connectionLock = new ReentrantLock();

    private final Condition condition = connectionLock.newCondition();

    /**
     * Ctor.
     * @param address
     */
    public WorkerClient(InetSocketAddress address) {
        this.address = address;
    }

    public boolean blockingInit() {

        Bootstrap workerBootstrap = new Bootstrap();

        workerBootstrap.group(workerGroup)
                .channel(NioSocketChannel.class)
                .remoteAddress(address)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {

                        ch.pipeline().addLast(
                                new LengthFieldBasedFrameDecoder(
                                        2*Constants.DEFAULT_CHUNK_SIZE_IN_KEYS*Long.BYTES,
                                        0,
                                        Long.BYTES,
                                        0,
                                        Long.BYTES
                                ),
                                new ByteBufToMsgDecoder(),
                                new MsgToByteEncoder(),
                                new BufferToArrayCodec(),
                                new ShuffleDecoder(),
                                new WorkerClientHandler(connectionLock, condition)
                        );

                    }
                });
        try {
            connectionLock.tryLock(TIMEOUT_IN_MS, TimeUnit.MILLISECONDS);
            ChannelFuture future = workerBootstrap.connect();
            Channel channel = future.sync().channel();
            log.info("Successfully connected to Master node. Try to join into cluster. Send GET_CONNECTION_MESSAGE.");
            future = channel.writeAndFlush(Message.getSimpleOutboundMessage(Message.Type.GET_CONNECTION));
            long time = System.currentTimeMillis();
            condition.await();
            time = System.currentTimeMillis() - time;
            log.info("Successfully start WorkerEntryPoint client. Waiting "+time+" ms");
            return true;
        }
        catch(InterruptedException e) {
            log.info("Can't start WorkerEntryPoint client");
            return false;
        }
        finally {
            connectionLock.unlock();
        }
    }

    public void stop() {
        workerGroup.shutdownGracefully();
    }

}
