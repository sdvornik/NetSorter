package com.yahoo.sdvornik.merger;

public final class LongArrayWrapper {
    private final long[] array;

    private final int generation;

    public LongArrayWrapper(int generation, long[] array) {
        this.generation = generation;
        this.array = array;
    }

    public long[] getArray() {
        return array;
    }

    public int getGeneration() {
        return generation;
    }
}
