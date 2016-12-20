package com.yahoo.sdvornik.sorter;

import java.util.Arrays;

/**
 * Implementation of non recursive MergingSort algorithm.
 */
public final class MergingSortNoRecursive extends Sorter {
    /**
     * Ctor.
     * @param longArr
     */
    public MergingSortNoRecursive(long[] longArr) {
        super(longArr);
    }

    public long[] sort() {
        int totalLength = longArr.length;

        for(
                int leftArrLength = 1;
                leftArrLength < totalLength;
                leftArrLength *= 2
        ) {
            for(
                    int leftArrStartIndex = 0;
                    leftArrStartIndex + leftArrLength < totalLength;
                    leftArrStartIndex += leftArrLength * 2
            ) {

                int rightArrLength = leftArrStartIndex + leftArrLength * 2 < totalLength ?
                        leftArrLength : (totalLength - (leftArrStartIndex + leftArrLength));

                long[] leftArr = Arrays.copyOfRange(
                        longArr,
                        leftArrStartIndex,
                        leftArrStartIndex + leftArrLength
                );

                long[] rightArr = Arrays.copyOfRange(
                        longArr,
                        leftArrStartIndex + leftArrLength,
                        leftArrStartIndex+ leftArrLength + rightArrLength
                );

                int leftIndex=0;
                int rightIndex=0;
                for (int i = 0; i < leftArr.length + rightArr.length; ++i) {
                    if (rightIndex < rightArrLength && leftIndex < leftArrLength) {
                        longArr[leftArrStartIndex + i] = (leftArr[leftIndex] > rightArr[rightIndex]) ?
                                rightArr[rightIndex++] : leftArr[leftIndex++];
                    }
                    else if (rightIndex < rightArrLength) {
                        longArr[leftArrStartIndex + i] = rightArr[rightIndex++];
                    }
                    else {
                        longArr[leftArrStartIndex + i] = leftArr[leftIndex++];
                    }
                }
            }
        }
        return longArr;
    }
}
