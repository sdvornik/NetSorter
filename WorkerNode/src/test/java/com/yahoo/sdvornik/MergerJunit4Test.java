package com.yahoo.sdvornik;

import com.yahoo.sdvornik.sorter.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;

public class MergerJunit4Test extends Assert {

    private final static int FIRST_ARRAY_LENGTH = 512*1024;
    private final static int SECOND_ARRAY_LENGTH = 256*1024;
    private final static int THIRD_ARRAY_LENGTH = 128*1024;

    private final long[] FIRST_ARRAY = new long[FIRST_ARRAY_LENGTH];
    private final long[] SECOND_ARRAY = new long[SECOND_ARRAY_LENGTH];
    private final long[] THIRD_ARRAY = new long[THIRD_ARRAY_LENGTH];

    private long[] RES_ARRAY;

    private long[][] MULTI_ARRAY = new long[][] {FIRST_ARRAY, SECOND_ARRAY, THIRD_ARRAY};

    private final static ThreadLocalRandom random = ThreadLocalRandom.current();

    @Before
    public void generateLongArray() {
        for(int i = 0; i < FIRST_ARRAY.length; ++i) {
            FIRST_ARRAY[i] = random.nextLong();
        }
        for(int i = 0; i < SECOND_ARRAY.length; ++i) {
            SECOND_ARRAY[i] = random.nextLong();
        }
        for(int i = 0; i < THIRD_ARRAY.length; ++i) {
            THIRD_ARRAY[i] = random.nextLong();
        }
    }

    @Test
    public void testMerger() {
        new QuickSort(FIRST_ARRAY).sort();
        new QuickSort(SECOND_ARRAY).sort();
        long start = System.nanoTime();
        RES_ARRAY = Merger.merge(FIRST_ARRAY, SECOND_ARRAY);
        long end = System.nanoTime();
        System.out.println("Merger : "+(end-start)/1000000+" ms");
        Assert.assertTrue(checkSortedArray());
    }

    @Test
    public void testMultiMerger() {
        new QuickSort(THIRD_ARRAY).sort();
        long start = System.nanoTime();
        RES_ARRAY = Merger.multiMerge(MULTI_ARRAY);
        long end = System.nanoTime();
        System.out.println("MultiMerger : "+(end-start)/1000000+" ms");
        Assert.assertTrue(checkSortedArray());
    }

    private boolean checkSortedArray() {
        boolean res = true;
        for(int i = 0; i < RES_ARRAY.length-1; ++i) {
            res &= RES_ARRAY[i] <= RES_ARRAY[i+1];
        }
        return res;
    }
}
