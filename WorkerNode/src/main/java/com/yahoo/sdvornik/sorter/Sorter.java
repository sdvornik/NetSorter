package com.yahoo.sdvornik.sorter;

import io.netty.buffer.ByteBuf;

import java.util.Arrays;

public abstract class Sorter {

    protected final long[] longArr;

    public Sorter(long[] longArr) {
        this.longArr = longArr;
    }

    public abstract long[] sort();

    protected void swapElmInLongArr(int i, int j){
        long temp = longArr[i];
        longArr[i] = longArr[j];
        longArr[j] = temp;
    }
}
