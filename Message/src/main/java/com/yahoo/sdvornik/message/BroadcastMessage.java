package com.yahoo.sdvornik.message;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Immutable class which represents master node connection data.
 */
public final class BroadcastMessage {

    private static final Logger log = LoggerFactory.getLogger(BroadcastMessage.class);

    public static BroadcastMessage getMsgFromBuffer(ByteBuf buffer) {

        byte[] byteArr = new byte[Integer.BYTES];
        buffer.readBytes(byteArr);

        try {
            InetAddress serverAddress = InetAddress.getByAddress(byteArr);
            int serverPort = buffer.readInt();
            return new BroadcastMessage(serverAddress, serverPort);
        }
        catch(UnknownHostException e) {
            log.error("Can't create BroadcastMessage", e);
            return null;
        }
    }

    private final InetAddress serverAddress;

    private final int serverPort;

    /**
     * Ctor.
     * @param serverAddress
     * @param serverPort
     */
    public BroadcastMessage(InetAddress serverAddress, int serverPort) {
        this.serverAddress = serverAddress;
        this.serverPort = serverPort;
    }

    public ByteBuf getByteBuf() {
        ByteBuf buffer = Unpooled.buffer(2*Integer.BYTES);
        buffer.writeBytes(serverAddress.getAddress());
        buffer.writeInt(serverPort);
        return buffer;
    }

    public InetAddress getServerAddress() {
        return serverAddress;
    }

    public int getServerPort() {
        return serverPort;
    }

}