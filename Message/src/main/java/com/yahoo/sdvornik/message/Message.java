package com.yahoo.sdvornik.message;

import io.netty.buffer.ByteBuf;

public abstract class Message {

    public static SimpleMessage getSimpleOutboundMessage(Type type) {
        return new SimpleMessage(type);
    }

    public abstract Type getType();

    public abstract long getLength();

    public void writeHeader(ByteBuf buf) {
        buf.writeLong(getLength());
        buf.writeInt(getType().getValue());
    }

    public abstract void writeContent(ByteBuf buf);

    public enum Type {
        DATA_TYPE(0),
        GET_CONNECTION(-1),
        CONNECTED(-2),
        START_SORTING(-3),
        TASK_TRANSMISSION_ENDED(-4),
        JOB_ENDED(-5),
        GET_RESULT(-6);

        private int value;

        Type(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

    }

    public static Type toEnum(int i) {
        Type res = null;
        switch(i) {
            case 0 :
                res = Type.DATA_TYPE;
                break;
            case -1 :
                res = Type.GET_CONNECTION;
                break;
            case -2 :
                res = Type.CONNECTED;
                break;
            case -3 :
                res = Type.START_SORTING;
                break;
            case -4 :
                res = Type.TASK_TRANSMISSION_ENDED;
                break;
            case -5 :
                res = Type.JOB_ENDED;
                break;
            case -6 :
                res = Type.GET_RESULT;
                break;
            default:
                throw new IllegalArgumentException();
        }
        return res;
    }
}
