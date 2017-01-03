package com.yahoo.sdvornik.message;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class DataMessageBuffer extends Message {

    private static final Logger log = LoggerFactory.getLogger(DataMessageBuffer.class.getName());

    private final ByteBuf nettyBuf;

    private final int chunkNumber;

    /**
     * Create DataMessageBuffer instance from file on Master Node
     * @param nioBuf
     * @param chunkNumber
     */
    public DataMessageBuffer(ByteBuffer nioBuf, int chunkNumber) {
        this.nettyBuf = Unpooled.wrappedBuffer(nioBuf);
        this.chunkNumber = chunkNumber;
    }

    /**
     * Create DataMessageBuffer instance in Decoder
     * @param nettyBuf
     * @param numberOfChunk
     */
    public DataMessageBuffer(ByteBuf nettyBuf, int numberOfChunk) {
        this.nettyBuf = nettyBuf;
        this.chunkNumber = numberOfChunk;
    }

    /**
     * Return chunk number
     * @return
     */
    public int getChunkNumber() {
        return chunkNumber;
    }

    public ByteBuf getNettyBuffer() {
        return nettyBuf;
    }

    @Override
    public Type getType() {
        return Type.DATA_TYPE;
    }

    @Override
    public long getLength() {
        return 2*Integer.BYTES + nettyBuf.readableBytes();
    }

    @Override
    public void writeContent(ByteBuf buf) {
        buf.writeInt(chunkNumber);
        buf.writeBytes(nettyBuf);
    }
}
