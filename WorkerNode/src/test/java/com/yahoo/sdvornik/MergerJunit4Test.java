package com.yahoo.sdvornik;

import com.yahoo.sdvornik.merger.Merger;
import com.yahoo.sdvornik.sorter.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.concurrent.ThreadLocalRandom;

public class MergerJunit4Test extends Assert {

    private final static int FIRST_ARRAY_LENGTH = 512*1024;
    private final static int SECOND_ARRAY_LENGTH = 256*1024;
    private final static int THIRD_ARRAY_LENGTH = 128*1024;

    private final long[] firstArray = new long[FIRST_ARRAY_LENGTH];
    private final long[] secondArray = new long[SECOND_ARRAY_LENGTH];
    private final long[] thirdArray = new long[THIRD_ARRAY_LENGTH];
    private final long[][] multiArray = new long[][] {firstArray, secondArray, thirdArray};

    private final ThreadLocalRandom random = ThreadLocalRandom.current();

    @Before
    public void generateArraysContent() {
        for(int i = 0; i < firstArray.length; ++i) {
            firstArray[i] = random.nextLong();
        }
        for(int i = 0; i < secondArray.length; ++i) {
            secondArray[i] = random.nextLong();
        }
        for(int i = 0; i < thirdArray.length; ++i) {
            thirdArray[i] = random.nextLong();
        }
    }

    @Test
    public void testMerger() throws Exception {
        new QuickSort(firstArray).sort();
        new QuickSort(secondArray).sort();

        Method method = Merger.class.getDeclaredMethod("merge", long[].class, long[].class);
        method.setAccessible(true);

        long start = System.nanoTime();
        long[] resArray = (long[])method.invoke(Merger.INSTANCE, firstArray, secondArray);
        //long[] resArray = Merger.INSTANCE.merge(firstArray, secondArray);
        long end = System.nanoTime();
        System.out.println("Merger : "+(end-start)/1000000+" ms");
        Assert.assertTrue(checkSortedArray(resArray));
    }

    @Test
    public void testMultiMerger() throws Exception {
        new QuickSort(thirdArray).sort();

        Method method = Merger.class.getDeclaredMethod("multiMerge", long[][].class);
        method.setAccessible(true);

        long start = System.nanoTime();
        long[] resArray = Merger.INSTANCE.multiMerge(multiArray);
        long end = System.nanoTime();
        System.out.println("MultiMerger : "+(end-start)/1000000+" ms");
        Assert.assertTrue(checkSortedArray(resArray));
    }

    private boolean checkSortedArray(long[] resArray) {
        boolean res = true;
        for(int i = 0; i < resArray.length-1; ++i) {
            res &= resArray[i] <= resArray[i+1];
        }
        return res;
    }
}
