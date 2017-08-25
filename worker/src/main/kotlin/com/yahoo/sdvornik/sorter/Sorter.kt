package com.yahoo.sdvornik.sorter

/**
 * @author Serg Dvornik <sdvornik@yahoo.com>
 */
abstract class Sorter(protected val longArr: LongArray) {

    abstract fun sort(): LongArray

    protected fun swap(i: Int, j: Int) {
        val temp = longArr[i]
        longArr[i] = longArr[j]
        longArr[j] = temp
    }
}