package com.yahoo.sdvornik;

public final class Constants {

    private Constants(){}

    public final static int BYTES_IN_KBYTES = 1024;

    public final static int BYTES_IN_MBYTES = 1048576;

    public final static int BROADCAST_PORT = 8394;

    public final static int WEBSOCKET_PORT = 6978;

    public final static int PORT = 7894;

    public final static int BROADCAST_INTERVAL_IN_MS = 1000;

    private final static int BUFFER_SIZE_IN_KBYTES = 128;

    public final static int MAX_FILE_SIZE_IN_MBYTES = 32;

    public final static int BUFFER_SIZE_IN_BYTES = BUFFER_SIZE_IN_KBYTES * BYTES_IN_KBYTES;

    public final static String DEFAULT_FILE_NAME = "presorted.txt";
}
