package com.yahoo.sdvornik.sorter

import io.netty.buffer.ByteBuf
import java.nio.ByteBuffer
import java.util.*

/**
 * @author Serg Dvornik <sdvornik@yahoo.com>
 */
class QuickSortByteArr(byteBuf: ByteBuf) {

    private val byteArr: ByteArray

    private val longBuffer = ByteBuffer.allocate(java.lang.Long.BYTES)

    init {
        this.byteArr = byteBuf.array()
    }

    fun sortByteBuf() {
        quickSort(0, byteArr.size / java.lang.Long.BYTES - 1)
    }

    /**
     * QuickSort recursive method for ByteBuf.
     * @param leftBorderIndex
     * @param rightBorderIndex
     */
    private fun quickSort(leftBorderIndex: Int, rightBorderIndex: Int) {
        if (leftBorderIndex >= rightBorderIndex) return
        val pivotElmIndex = getPivotElmIndex(leftBorderIndex, rightBorderIndex)
        quickSort(leftBorderIndex, pivotElmIndex - 1)
        quickSort(pivotElmIndex + 1, rightBorderIndex)
    }

    /**
     * Return new pivot element index in [startIndex, endIndex] for long[].
     * @param startIndex
     * @param endIndex
     * @return
     */
    private fun getPivotElmIndex(startIndex: Int, endIndex: Int): Int {
        var longBytes = Arrays.copyOfRange(byteArr, endIndex * java.lang.Long.BYTES, (1 + endIndex) * java.lang.Long.BYTES)

        longBuffer.put(longBytes)
        longBuffer.flip()
        val pivotElmValue = longBuffer.long
        longBuffer.clear()
        var pivotElmIndex = startIndex
        for (i in startIndex..endIndex - 1) {
            longBytes = Arrays.copyOfRange(byteArr, i * java.lang.Long.BYTES, (1 + i) * java.lang.Long.BYTES)
            longBuffer.put(longBytes)
            longBuffer.flip()
            val elm = longBuffer.long
            longBuffer.clear()
            if (elm >= pivotElmValue) continue
            swapElmInByteArr(pivotElmIndex, i)
            ++pivotElmIndex
        }
        swapElmInByteArr(pivotElmIndex, endIndex)
        return pivotElmIndex
    }

    protected fun swapElmInByteArr(i: Int, j: Int) {
        val tempI = Arrays.copyOfRange(byteArr, i * java.lang.Long.BYTES, (i + 1) * java.lang.Long.BYTES)
        val tempJ = Arrays.copyOfRange(byteArr, j * java.lang.Long.BYTES, (j + 1) * java.lang.Long.BYTES)
        for (k in 0..java.lang.Long.BYTES - 1) {
            byteArr[i * java.lang.Long.BYTES + k] = tempJ[k]
            byteArr[j * java.lang.Long.BYTES + k] = tempI[k]
        }
    }
}
