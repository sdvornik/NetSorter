package com.yahoo.sdvornik.sorter;

public final class Merger {

    private Merger(){}

    public static long[] merge(long[] firstArr, long[] secondArr) {
        long[] resArr = new long[firstArr.length+secondArr.length];
        int firstIndex=0;
        int secondIndex=0;

        for (int i = 0; i < resArr.length; ++i) {
            if (firstIndex < firstArr.length && secondIndex < secondArr.length) {
                resArr[i] = (firstArr[firstIndex] < secondArr[secondIndex]) ?
                        firstArr[firstIndex++] : secondArr[secondIndex++];
            }
            else if (firstIndex < firstArr.length) {
                resArr[i] = firstArr[firstIndex++];
            }
            else {
                resArr[i] = secondArr[secondIndex++];
            }
        }
        return resArr;
    }

    public static long[] multiMerge(long[][] arr) {
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
