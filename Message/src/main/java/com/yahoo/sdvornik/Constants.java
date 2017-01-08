package com.yahoo.sdvornik;

public final class Constants {

    private Constants() {
    }

    public final static int BYTES_IN_KBYTES = 1024;

    public final static long BYTES_IN_MBYTES = 1048576;

    public final static int BROADCAST_PORT = 8394;

    public final static int WEBSOCKET_PORT = 6978;

    public final static int PORT = 7894;

    public final static int BROADCAST_INTERVAL_IN_MS = 1000;

    private final static int BUFFER_SIZE_IN_KBYTES = 128;

    public final static int MAX_FILE_SIZE_IN_MBYTES = 512;

    public final static int BUFFER_SIZE_IN_BYTES = BUFFER_SIZE_IN_KBYTES * BYTES_IN_KBYTES;

    public final static int DEFAULT_CHUNK_SIZE_IN_KEYS = 64 * 1024 / 8;

    public final static int RESULT_CHUNK_SIZE_IN_KEYS = DEFAULT_CHUNK_SIZE_IN_KEYS;

    public final static String DEFAULT_FILE_NAME = "presorted.txt";

    public final static String OUTPUT_FILE_NAME = "sorted.txt";

    public final static String TEST_OUTPUT_FILE_NAME = "sorted_test.txt";
}
