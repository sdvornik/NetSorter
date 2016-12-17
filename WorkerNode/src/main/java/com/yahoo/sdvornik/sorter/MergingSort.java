package com.yahoo.sdvornik.sorter;

import java.util.Arrays;

public final class MergingSort extends Sorter {

    public MergingSort(long[] longArr) {
        super(longArr);
    }

    public void sort(){
        mergingSortRecursive(0, longArr.length-1);
    }

    /**
     * Sorting recursive function
     * @param A - массив int
     * @param leftBorderIndex - left border
     * @param rightBorderIndex - right border
     */
    private void mergingSortRecursive(int leftBorderIndex, int rightBorderIndex ) {

        if (leftBorderIndex >= rightBorderIndex) return;

        int splitIndex = (leftBorderIndex + rightBorderIndex )/2;
        mergingSortRecursive(leftBorderIndex, splitIndex);
        mergingSortRecursive(splitIndex + 1, rightBorderIndex);
        mergeArrays(leftBorderIndex, rightBorderIndex, splitIndex);
    }

    /**
     * Merging of two sorted array
     * @param A - long array
     * @param leftBorderIndex - left border of current array
     * @param rightBorderIndex - right border of current arrray
     * @param splitIndex - split point of current array, including in left array
     */
    private void mergeArrays(int leftBorderIndex, int rightBorderIndex, int splitIndex) {

        long[] leftArr = Arrays.copyOfRange(longArr, leftBorderIndex, splitIndex+1);

        long[] rightArr =  Arrays.copyOfRange(longArr, splitIndex+1, rightBorderIndex+1);

        int leftArrLength = leftArr.length;
        int rightArrLength = rightArr.length;
        int totalLength = leftArrLength + rightArrLength;

        int leftIndex=0;
        int rightIndex=0;

        for (int i = leftBorderIndex; i < leftBorderIndex+totalLength; ++i) {
            if (rightIndex < rightArrLength && leftIndex < leftArrLength) {
                longArr[i] = (leftArr[leftIndex] > rightArr[rightIndex]) ?
                        rightArr[rightIndex++] : leftArr[leftIndex++];
            }
            else if (rightIndex < rightArrLength) {
                longArr[i] = rightArr[rightIndex++];
            }
            else {
                longArr[i] = leftArr[leftIndex++];
            }
        }
    }
}
	

