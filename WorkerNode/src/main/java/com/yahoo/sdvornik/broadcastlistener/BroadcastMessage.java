package com.yahoo.sdvornik.broadcastlistener;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

public final class BroadcastMessage {

    private static final Logger log = LoggerFactory.getLogger(BroadcastMessage.class.getName());

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

    public BroadcastMessage(InetAddress serverAddress, int serverPort) {
        this.serverAddress = serverAddress;
        this.serverPort = serverPort;
    }

    public byte[] getByteArray() {
        ByteBuf buffer = Unpooled.buffer(2*Integer.BYTES);
        buffer.writeBytes(serverAddress.getAddress());
        buffer.writeInt(serverPort);
        byte[] byteArr = buffer.array();
        return byteArr;
    }

    public InetAddress getServerAddress() {
        return serverAddress;
    }

    public int getServerPort() {
       return serverPort;
    }

}

