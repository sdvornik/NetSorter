package com.yahoo.sdvornik.merger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public enum Merger {

    INSTANCE;

    private final Logger log = LoggerFactory.getLogger(Merger.class.getName());

    private final ExecutorService executor = Executors.newFixedThreadPool(1);

    public long[] multiMerge(long[][] arr) {
        int mergeArrLength = 0;
        for(int numberOfArr = 0; numberOfArr < arr.length; ++numberOfArr) {
            mergeArrLength+= arr[numberOfArr].length;
        }
        long[] mergeArr = new long[mergeArrLength];
        int[] curIndex = new int[arr.length];
        long curMinValue = Long.MAX_VALUE;
        int curNumberOfArrWithMinValue = -1;
        int firstIndex=0;
        int secondIndex=0;

        for (int i = 0; i < mergeArrLength; ++i) {
            int indexOfArrWithMinValue = 0;
            for(int k = 0; k < arr.length; ++k) {
                if(curIndex[k] < arr[k].length && arr[k][curIndex[k]] < curMinValue) {
                    curMinValue = arr[k][curIndex[k]];
                    curNumberOfArrWithMinValue = k;
                }
            }
            ++curIndex[curNumberOfArrWithMinValue];
            mergeArr[i] = curMinValue;
        }
        return mergeArr;
    }
}
