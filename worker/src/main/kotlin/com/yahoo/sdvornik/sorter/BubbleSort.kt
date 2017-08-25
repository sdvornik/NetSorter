package com.yahoo.sdvornik.sorter

/**
 * @author Serg Dvornik <sdvornik@yahoo.com>
 */
class BubbleSort
/**
 * Ctor.
 * @param longArr
 */
(longArr: LongArray) : Sorter(longArr) {

    override fun sort(): LongArray {
        for (i in longArr.indices) {
            for (j in 0..longArr.size - i - 1 - 1) {
                if (longArr[j] > longArr[j + 1]) swap(j, j + 1)
            }
        }
        return longArr
    }
}