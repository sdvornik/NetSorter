package com.yahoo.sdvornik;

import com.yahoo.sdvornik.master.Merger;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class MultiMergerJUnit4Test {

    private int NUMBER_OF_KEYS_FOR_ONE_NODE = 25;

    private int ARRAY_SIZE = 5;

    private final String[] id = new String[]{"id1", "id2", "id3"};

    private final fj.data.List<String> idList = fj.data.List.iterableList(Arrays.asList(id));

    private final long[] mergedArray = new long[NUMBER_OF_KEYS_FOR_ONE_NODE*id.length];

    private Merger merger =  new Merger(
            idList,
            NUMBER_OF_KEYS_FOR_ONE_NODE*idList.length(),
            ARRAY_SIZE*idList.length(),
            mergedArray,
            null,
            null,
            null
    );

    @Test
    public void testMultiMerger() throws Exception {

        Thread mainThread = new Thread(new Runnable() {
            @Override
            public void run() {
                merger.init();
                Thread t_1 = new Thread(new ArrayGenerator(0));
                t_1.start();
                Thread t_2 = new Thread(new ArrayGenerator(1));
                t_2.start();
                Thread t_3 = new Thread(new ArrayGenerator(2));
                t_3.start();
                try {
                    t_1.join();
                    t_2.join();
                    t_3.join();
                }
                catch(InterruptedException e) {
                    Thread.currentThread().interrupt();
                }

            }
        });
        mainThread.start();
        mainThread.join();
        Assert.assertTrue(checkSortedArray(mergedArray));
    }

    public class ArrayGenerator implements Runnable {
        private final int group;

        public ArrayGenerator(int group) {
            this.group = group;
        }
        @Override
        public void run() {
            int TOTAL_ARRAY_AMOUNT = NUMBER_OF_KEYS_FOR_ONE_NODE/ARRAY_SIZE;

            for(int numberOfChunk=0; numberOfChunk<TOTAL_ARRAY_AMOUNT; ++numberOfChunk) {
                long[] arr = new long[ARRAY_SIZE];
                for (int i = 0; i < ARRAY_SIZE; ++i) {
                    arr[i] = numberOfChunk*ARRAY_SIZE*idList.length() + idList.length()*(i+1) - group;
                }

                merger.putArrayInQueue(id[group], numberOfChunk, arr);
            }
        }
    }

    private boolean checkSortedArray(long[] sortedArr) {
        boolean res = true;
        for(int i = 0; i < sortedArr.length-1; ++i) {
            res &= sortedArr[i] <= sortedArr[i+1];
        }
        return res;
    }
}
