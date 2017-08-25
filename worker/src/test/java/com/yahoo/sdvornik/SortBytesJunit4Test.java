package com.yahoo.sdvornik;

import com.yahoo.sdvornik.sorter.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;

public class SortBytesJunit4Test {

    private final static int ARRAY_LENGTH = 1024*1024;

    private final static int BUF_CAPACITY = ARRAY_LENGTH*Long.BYTES;

    private final ByteBuf byteBuf = Unpooled.buffer(BUF_CAPACITY);

    private final ThreadLocalRandom random = ThreadLocalRandom.current();

    @Before
    public void generateByteBufContent() {
        for(int i = 0; i < ARRAY_LENGTH; ++i) {
            byteBuf.writeLong(random.nextLong());
        }
    }

    @After
    public void clearLongArray() {
        byteBuf.clear();
    }

    @Test
    public void testQuickSort() throws Exception{
        QuickSortByteArr sorter = new QuickSortByteArr(byteBuf);

        long start = System.nanoTime();
        sorter.sortByteBuf();
        long end = System.nanoTime();
        Assert.assertTrue(checkSortedByteBuf());
        System.out.println("ByteBuf sorter : "+(end-start)/1000000+" ms");
    }

    @Test
    public void convertByteBufToLongArr() throws Exception{
        long start = System.nanoTime();
        long[] longArr = new long[ARRAY_LENGTH];
        for(int i = 0; i<longArr.length; ++i) {
            longArr[i] = byteBuf.readLong();
        }
        long end = System.nanoTime();

        System.out.println("Read ByteBuf to long[] " + (end-start)/1000000+" ms");
    }

    private boolean checkSortedByteBuf() {
        boolean res = true;
        for(int i = 0; i < ARRAY_LENGTH-1; ++i) {
            res &= byteBuf.getLong(i*Long.BYTES) <= byteBuf.getLong((i+1)*Long.BYTES);
        }
        return res;
    }
}
