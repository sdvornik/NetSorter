package com.yahoo.sdvornik.message;

import io.netty.buffer.ByteBuf;

public class StartSortingMessage  extends Message {
    private int content;

    public int getContent() {
        return content;
    }

    public StartSortingMessage(int content) {
        this.content = content;
    }

    @Override
    public Type getType() {
        return Type.START_SORTING;
    }

    @Override
    public long getLength() {
        return 2*Integer.BYTES;
    }

    @Override
    public void writeContent(ByteBuf buf) {
        buf.writeInt(content);
    }
}
