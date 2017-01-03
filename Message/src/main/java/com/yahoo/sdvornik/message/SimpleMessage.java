package com.yahoo.sdvornik.message;

import io.netty.buffer.ByteBuf;

public class SimpleMessage extends Message {

    private final Type type;

    protected SimpleMessage(Type type) {
        this.type = type;
    }

    @Override
    public Type getType() {
        return type;
    }

    @Override
    public long getLength() {
        return Integer.BYTES;
    }

    @Override
    public void writeContent(ByteBuf buf) {
        throw new UnsupportedOperationException("Can't write content for SimpleMessage");
    }
}
