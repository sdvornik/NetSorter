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

    private final static int ARRAY_LENGTH = 1024*1024;

    private final ThreadLocalRandom random = ThreadLocalRandom.current();

    private final long[] testArray = new long[ARRAY_LENGTH];

    @Before
    public void generateLongArray() {
        for(int i = 0; i < testArray.length; ++i) {
            testArray[i] = random.nextLong();
        }
    }

    @After
    public void clearLongArray() {
        Arrays.fill(testArray,0L);
    }
/*
    @Test
    public void testBubbleSort() throws Exception{
        commonTest(BubbleSort.class);
    }

    @Test
    public void testInsertionSort() throws Exception{
        commonTest(InsertionSort.class);
    }
*/
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
        Sorter sorter = (Sorter)ctor.newInstance(new Object[]{testArray});

        long start = System.nanoTime();
        long[] sortedArr = sorter.sort();
        long end = System.nanoTime();
        Assert.assertTrue(checkSortedArray(sortedArr));
        System.out.println("Algorithm "+clazz.getSimpleName()+": "+(end-start)/1000000+" ms");
    }

    private boolean checkSortedArray(long[] sortedArr) {
        boolean res = true;
        for(int i = 0; i < sortedArr.length-1; ++i) {
            res &= sortedArr[i] <= sortedArr[i+1];
        }
        return res;
    }
}
