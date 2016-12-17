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
}
