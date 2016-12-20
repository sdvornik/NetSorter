package com.yahoo.sdvornik.sharable;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public enum MasterWorkerMessage {

    GET_CONNECTION(-1, false),
    CONNECTED(-2, false),
    START_SORTING(-3, true),
    STOP_TASK_TRANSMISSION(-4, false),
    JOB_ENDED(-5, false),
    GET_RESULT(-6, false);

    private int value;
    private boolean isIntPayload;

    MasterWorkerMessage(int value, boolean isIntPayload) {
        this.value = value;
        this.isIntPayload = isIntPayload;
    }

    public int getValue() {
        return value;
    }

    public ByteBuf getByteBuf() {
        if(!this.isIntPayload) {
            ByteBuf buffer = Unpooled.buffer(Long.BYTES+Integer.BYTES);
            buffer.writeLong(Integer.BYTES);
            buffer.writeInt(value);
            return buffer;
        }
        else {
            throw new UnsupportedOperationException();
        }
    }

    public ByteBuf getByteBuf(int payload) {
        if(this.isIntPayload) {
            ByteBuf buffer = Unpooled.buffer(Long.BYTES+2*Integer.BYTES);
            buffer.writeLong(2*Integer.BYTES);
            buffer.writeInt(value);
            buffer.writeInt(payload);
            return buffer;
        }
        else {
            throw new UnsupportedOperationException();
        }
    }

    public int readIntPayload(ByteBuf buffer) {
        if(this.isIntPayload) {
            return buffer.getInt(Integer.BYTES);
        }
        else {
            throw new UnsupportedOperationException();
        }
    }

    public static MasterWorkerMessage toEnum(int i) {
        MasterWorkerMessage res = null;
        switch(i) {
            case -1 :
                res = GET_CONNECTION;
                break;
            case -2 :
                res = CONNECTED;
                break;
            case -3 :
                res = START_SORTING;
                break;
            case -4 :
                res = STOP_TASK_TRANSMISSION;
                break;
            case -5 :
                res = JOB_ENDED;
                break;
            case -6 :
                res = GET_RESULT;
                break;
        }
        return res;
    }

}
