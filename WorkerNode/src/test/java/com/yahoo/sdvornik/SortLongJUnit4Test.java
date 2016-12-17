package com.yahoo.sdvornik;

import com.yahoo.sdvornik.sorter.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

public class SortLongJUnit4Test extends Assert {

    private final static int ARRAY_LENGTH = 256;//1024*1024;

    private final ThreadLocalRandom random = ThreadLocalRandom.current();

    private final long[] TEST_ARRAY = new long[ARRAY_LENGTH];

    @Before
    public void generateLongArray() {
        for(int i = 0; i < TEST_ARRAY.length; ++i) {
            TEST_ARRAY[i] = random.nextLong();
        }
    }

    @After
    public void clearLongArray() {
        Arrays.fill(TEST_ARRAY,0L);
    }

    @Test
    public void testBubbleSort() throws Exception{
        commonTest(BubbleSort.class);
    }

    @Test
    public void testInsertionSort() throws Exception{
        commonTest(InsertionSort.class);
    }

    @Test
    public void testMergingSort() throws Exception{
        commonTest(MergingSort.class);
    }

    @Test
    public void testMergingSortNoRecursive() throws Exception{
        commonTest(MergingSortNoRecursive.class);
    }

    @Test
    public void testQuickSort() throws Exception{
        commonTest(QuickSort.class);
    }

    private void commonTest(Class<?> clazz) throws Exception {
        if(!Sorter.class.isAssignableFrom(clazz)) {
            throw new IllegalArgumentException("Class " + clazz.getSimpleName() + " not implement interface Sorter");
        };

        Constructor<?> ctor = clazz.getConstructor(long[].class);
        Sorter sorter = (Sorter)ctor.newInstance(new Object[]{TEST_ARRAY});

        long start = System.nanoTime();
        sorter.sort();
        long end = System.nanoTime();
        Assert.assertTrue(checkSortedArray());
        System.out.println("Algorithm "+clazz.getSimpleName()+": "+(end-start)/1000000+" ms");
    }

    private boolean checkSortedArray() {
        boolean res = true;
        for(int i = 0; i < TEST_ARRAY.length-1; ++i) {
            res &= TEST_ARRAY[i] <= TEST_ARRAY[i+1];
        }
        return res;
    }
}
