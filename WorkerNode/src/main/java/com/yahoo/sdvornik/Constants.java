package com.yahoo.sdvornik;

public final class Constants {

    private Constants(){}

    public final static int BROADCAST_PORT = 8394;

    public final static int PORT = 7894;

    public final static int BROADCAST_INTERVAL_IN_MS = 1000;

    public final static int DEFAULT_CHUNK_SIZE_IN_KEYS = 64*1024/8;

    public final static int GET_CONNECTION = -1;

    public final static int CONNECTED = -2;

    public final static int START_SORTING = -3;
}
