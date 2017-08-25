package com.yahoo.sdvornik.sorter

/**
 * @author Serg Dvornik <sdvornik@yahoo.com>
 */
class InsertionSort
/**
 * Ctor.
 * @param longArr
 */
(longArr: LongArray) : Sorter(longArr) {

    override fun sort(): LongArray {
        for (j in 1..longArr.size - 1) {
            val key = longArr[j]
            var i = j - 1
            while (i >= 0 && longArr[i] > key) {
                longArr[i + 1] = longArr[i]
                --i
            }
            longArr[i + 1] = key
        }
        return longArr
    }
}