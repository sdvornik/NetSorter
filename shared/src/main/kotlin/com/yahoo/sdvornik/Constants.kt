package com.yahoo.sdvornik

/**
 * @author Serg Dvornik <sdvornik@yahoo.com>
 */
object Constants {

    @JvmField val BYTES_IN_KBYTES = 1024

    @JvmField val BYTES_IN_MBYTES: Long = 1048576

    @JvmField val BROADCAST_PORT = 8394

    @JvmField val WEBSOCKET_PORT = 6978

    @JvmField val PORT = 7894

    @JvmField val BROADCAST_INTERVAL_IN_MS = 1000

    private val BUFFER_SIZE_IN_KBYTES = 128

    @JvmField val MAX_FILE_SIZE_IN_MBYTES = 512

    @JvmField val BUFFER_SIZE_IN_BYTES = BUFFER_SIZE_IN_KBYTES * BYTES_IN_KBYTES

    @JvmField val DEFAULT_CHUNK_SIZE_IN_KEYS = 64 * 1024 / 8

    @JvmField val RESULT_CHUNK_SIZE_IN_KEYS = DEFAULT_CHUNK_SIZE_IN_KEYS

    @JvmField val DEFAULT_FILE_NAME = "presorted.txt"

    @JvmField val OUTPUT_FILE_NAME = "sorted.txt"

    @JvmField val TEST_OUTPUT_FILE_NAME = "sorted_test.txt"
}
