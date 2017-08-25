package com.yahoo.sdvornik.sorter

/**
 * @author Serg Dvornik <sdvornik@yahoo.com>
 */
class QuickSort
/**
 * Ctor.
 * @param longArr
 */
(longArr: LongArray) : Sorter(longArr) {

    override fun sort(): LongArray {
        quickSort(0, longArr.size - 1)
        return longArr
    }

    /**
     * Return new pivot element index in [startIndex, endIndex] for long[].
     *
     * @param startIndex
     * @param endIndex
     * @return
     */
    private fun getPivotElmIndex(startIndex: Int, endIndex: Int): Int {
        val pivotElmValue = longArr[endIndex]
        var pivotElmIndex = startIndex
        for (i in startIndex..endIndex - 1) {
            if (longArr[i] >= pivotElmValue) continue
            swap(pivotElmIndex, i)
            ++pivotElmIndex
        }
        swap(pivotElmIndex, endIndex)
        return pivotElmIndex
    }

    /**
     * QuickSort recursive method for long[].
     *
     * @param leftBorderIndex
     * @param rightBorderIndex
     */
    private fun quickSort(leftBorderIndex: Int, rightBorderIndex: Int) {
        if (leftBorderIndex >= rightBorderIndex) return
        val pivotElmIndex = getPivotElmIndex(leftBorderIndex, rightBorderIndex)
        quickSort(leftBorderIndex, pivotElmIndex - 1)
        quickSort(pivotElmIndex + 1, rightBorderIndex)
    }
}