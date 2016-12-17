package com.yahoo.sdvornik;

import com.yahoo.sdvornik.sorter.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.util.concurrent.ThreadLocalRandom;

public class SortBytesJunit4Test {
    private final static int ARRAY_LENGTH = 1024*1024;
    private final ByteBuf TEST_BYTE_BUF = Unpooled.buffer(ARRAY_LENGTH*Long.BYTES);
    private final ThreadLocalRandom random = ThreadLocalRandom.current();

    @Before
    public void generateLongArray() {
        for(int i = 0; i < ARRAY_LENGTH; ++i) {
            TEST_BYTE_BUF.writeLong(random.nextLong());
        }
    }

    @After
    public void clearLongArray() {
        TEST_BYTE_BUF.clear();
    }

    @Test
    public void testQuickSort() throws Exception{
        QuickSortByteArr sorter = new QuickSortByteArr(TEST_BYTE_BUF);

        long start = System.nanoTime();
        sorter.sortByteBuf();
        long end = System.nanoTime();
        Assert.assertTrue(checkSortedArray());
        System.out.println("ByteBuf sorter : "+(end-start)/1000000+" ms");
    }

    @Test
    public void convertByteBufToLongArr() throws Exception{
        long start = System.nanoTime();
        long[] longArr = new long[ARRAY_LENGTH];
        for(int i = 0; i<ARRAY_LENGTH; ++i) {
            longArr[i] = TEST_BYTE_BUF.readLong();
        }
        long end = System.nanoTime();

        System.out.println("Convertion " + (end-start)/1000000+" ms");
    }

    private boolean checkSortedArray() {
        boolean res = true;
        for(int i = 0; i < ARRAY_LENGTH-1; ++i) {
            res &= TEST_BYTE_BUF.getLong(i*Long.BYTES) <= TEST_BYTE_BUF.getLong((i+1)*Long.BYTES);
        }
        return res;
    }
}
