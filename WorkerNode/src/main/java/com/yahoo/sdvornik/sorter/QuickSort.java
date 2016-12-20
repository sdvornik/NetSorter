package com.yahoo.sdvornik.sorter;

/**
 * Implementation of QuickSort algorithm.
 */
public final class QuickSort extends Sorter {

    /**
     * Ctor.
     * @param longArr
     */
    public QuickSort(long[] longArr) {
        super(longArr);
    }

    public long[] sort() {
        quickSort(0, longArr.length - 1);
        return longArr;
    }

    /**
     * Return new pivot element index in [startIndex, endIndex] for long[].
     *
     * @param startIndex
     * @param endIndex
     * @return
     */
    private int getPivotElmIndex(int startIndex, int endIndex) {
        long pivotElmValue = longArr[endIndex];
        int pivotElmIndex = startIndex;
        for (int i = startIndex; i < endIndex; ++i) {
            if (longArr[i] >= pivotElmValue) continue;
            swap(pivotElmIndex, i);
            ++pivotElmIndex;
        }
        swap(pivotElmIndex, endIndex);
        return pivotElmIndex;
    }

    /**
     * QuickSort recursive method for long[].
     *
     * @param leftBorderIndex
     * @param rightBorderIndex
     */
    private void quickSort(int leftBorderIndex, int rightBorderIndex) {
        if (leftBorderIndex >= rightBorderIndex) return;
        int pivotElmIndex = getPivotElmIndex(leftBorderIndex, rightBorderIndex);
        quickSort(leftBorderIndex, pivotElmIndex - 1);
        quickSort(pivotElmIndex + 1, rightBorderIndex);
    }
}

