package com.yahoo.sdvornik.sorter

import java.util.*

/**
 * @author Serg Dvornik <sdvornik@yahoo.com>
 */
class MergingSortNoRecursive
/**
 * Ctor.
 * @param longArr
 */
(longArr: LongArray) : Sorter(longArr) {

    override fun sort(): LongArray {
        val totalLength = longArr.size

        var leftArrLength = 1
        while (leftArrLength < totalLength) {
            var leftArrStartIndex = 0
            while (leftArrStartIndex + leftArrLength < totalLength) {

                val rightArrLength = if (leftArrStartIndex + leftArrLength * 2 < totalLength)
                    leftArrLength
                else
                    totalLength - (leftArrStartIndex + leftArrLength)

                val leftArr = Arrays.copyOfRange(
                        longArr,
                        leftArrStartIndex,
                        leftArrStartIndex + leftArrLength
                )

                val rightArr = Arrays.copyOfRange(
                        longArr,
                        leftArrStartIndex + leftArrLength,
                        leftArrStartIndex + leftArrLength + rightArrLength
                )

                var leftIndex = 0
                var rightIndex = 0
                for (i in 0..leftArr.size + rightArr.size - 1) {
                    if (rightIndex < rightArrLength && leftIndex < leftArrLength) {
                        longArr[leftArrStartIndex + i] = if (leftArr[leftIndex] > rightArr[rightIndex])
                            rightArr[rightIndex++]
                        else
                            leftArr[leftIndex++]
                    } else if (rightIndex < rightArrLength) {
                        longArr[leftArrStartIndex + i] = rightArr[rightIndex++]
                    } else {
                        longArr[leftArrStartIndex + i] = leftArr[leftIndex++]
                    }
                }
                leftArrStartIndex += leftArrLength * 2
            }
            leftArrLength *= 2
        }
        return longArr
    }
}