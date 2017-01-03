package com.yahoo.sdvornik.message;

public class DataMessageArray {

    private final long[] arr;

    private final int chunkNumber;

    public DataMessageArray(long[] arr, int chunkNumber) {
        this.arr = arr;
        this.chunkNumber = chunkNumber;
    }

    public long[] getArray() {
        return arr;
    }

    public int getChunkNumber() {
        return chunkNumber;
    }
}
