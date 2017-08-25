package com.yahoo.sdvornik.sorter

import java.util.*

/**
 * @author Serg Dvornik <sdvornik@yahoo.com>
 */
/**
 * Implementation of MergingSort algorithm.
 */
class MergingSort
/**
 * Ctor.
 * @param longArr
 */
(longArr: LongArray) : Sorter(longArr) {

    override fun sort(): LongArray {
        mergingSortRecursive(0, longArr.size - 1)
        return longArr
    }

    /**
     * Sorting recursive function
     * @param A - массив int
     * @param leftBorderIndex - left border
     * @param rightBorderIndex - right border
     */
    private fun mergingSortRecursive(leftBorderIndex: Int, rightBorderIndex: Int) {

        if (leftBorderIndex >= rightBorderIndex) return

        val splitIndex = (leftBorderIndex + rightBorderIndex) / 2
        mergingSortRecursive(leftBorderIndex, splitIndex)
        mergingSortRecursive(splitIndex + 1, rightBorderIndex)
        mergeArrays(leftBorderIndex, rightBorderIndex, splitIndex)
    }

    /**
     * Merging of two sorted array
     * @param A - long array
     * @param leftBorderIndex - left border of current array
     * @param rightBorderIndex - right border of current arrray
     * @param splitIndex - split point of current array, including in left array
     */
    private fun mergeArrays(leftBorderIndex: Int, rightBorderIndex: Int, splitIndex: Int) {

        val leftArr = Arrays.copyOfRange(longArr, leftBorderIndex, splitIndex + 1)

        val rightArr = Arrays.copyOfRange(longArr, splitIndex + 1, rightBorderIndex + 1)

        val leftArrLength = leftArr.size
        val rightArrLength = rightArr.size
        val totalLength = leftArrLength + rightArrLength

        var leftIndex = 0
        var rightIndex = 0

        for (i in leftBorderIndex..leftBorderIndex + totalLength - 1) {
            if (rightIndex < rightArrLength && leftIndex < leftArrLength) {
                longArr[i] = if (leftArr[leftIndex] > rightArr[rightIndex])
                    rightArr[rightIndex++]
                else
                    leftArr[leftIndex++]
            } else if (rightIndex < rightArrLength) {
                longArr[i] = rightArr[rightIndex++]
            } else {
                longArr[i] = leftArr[leftIndex++]
            }
        }
    }
}