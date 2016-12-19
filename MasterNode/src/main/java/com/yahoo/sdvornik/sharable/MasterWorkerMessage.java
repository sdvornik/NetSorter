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
    private ByteBuf buffer;

    MasterWorkerMessage(int value, boolean isIntPayload) {
        this.value = value;
        this.isIntPayload = isIntPayload;
        this.buffer = Unpooled.buffer(Long.BYTES+Integer.BYTES + (this.isIntPayload ? Integer.BYTES : 0));
        if(!this.isIntPayload) {
            this.buffer.writeLong(Integer.BYTES + (this.isIntPayload ? Integer.BYTES : 0));
            this.buffer.writeInt(value);
        }
    }

    public int getValue() {
        return value;
    }

    public ByteBuf getByteBuf() {
        return buffer;
    }

    public void setIntPayload(int payload) {
        if(this.isIntPayload) {
            this.buffer.clear();
            this.buffer.writeLong(Integer.BYTES + Integer.BYTES);
            this.buffer.writeInt(value);
            this.buffer.writeInt(payload);
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
