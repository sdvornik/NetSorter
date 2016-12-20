package com.yahoo.sdvornik.sorter;

import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Implementation of QuickSort algorithm for long keys in byte array.
 * For testing purposes.
 */
public class QuickSortByteArr {

    private final byte[] byteArr;

    private final ByteBuffer longBuffer = ByteBuffer.allocate(Long.BYTES);

    public QuickSortByteArr(ByteBuf byteBuf) {
        this.byteArr = byteBuf.array();
    }

    public void sortByteBuf() {
        quickSort(0 , byteArr.length/Long.BYTES-1);
    }

    /**
     * QuickSort recursive method for ByteBuf.
     * @param leftBorderIndex
     * @param rightBorderIndex
     */
    private void quickSort(int leftBorderIndex , int rightBorderIndex) {
        if (leftBorderIndex >= rightBorderIndex) return;
        int pivotElmIndex = getPivotElmIndex(leftBorderIndex, rightBorderIndex);
        quickSort(leftBorderIndex, pivotElmIndex - 1);
        quickSort(pivotElmIndex + 1, rightBorderIndex);
    }

    /**
     * Return new pivot element index in [startIndex, endIndex] for long[].
     * @param startIndex
     * @param endIndex
     * @return
     */
    private int getPivotElmIndex(int startIndex, int endIndex ){
        byte[] longBytes = Arrays.copyOfRange(byteArr, endIndex*Long.BYTES, (1+endIndex)*Long.BYTES);

        longBuffer.put(longBytes);
        longBuffer.flip();
        long pivotElmValue = longBuffer.getLong();
        longBuffer.clear();
        int pivotElmIndex = startIndex;
        for(int i = startIndex; i<endIndex; ++i){
            longBytes = Arrays.copyOfRange(byteArr, i*Long.BYTES, (1+i)*Long.BYTES);
            longBuffer.put(longBytes);
            longBuffer.flip();
            long elm = longBuffer.getLong();
            longBuffer.clear();
            if (elm >= pivotElmValue) continue;
            swapElmInByteArr(pivotElmIndex, i);
            ++pivotElmIndex;
        }
        swapElmInByteArr(pivotElmIndex,endIndex);
        return pivotElmIndex;
    }

    protected void swapElmInByteArr(int i, int j){
        byte[] tempI = Arrays.copyOfRange(byteArr, i*Long.BYTES, (i+1)*Long.BYTES);
        byte[] tempJ = Arrays.copyOfRange(byteArr, j*Long.BYTES, (j+1)*Long.BYTES);
        for(int k = 0; k < Long.BYTES; ++k) {
            byteArr[i*Long.BYTES+k] = tempJ[k];
            byteArr[j*Long.BYTES+k] = tempI[k];
        }
    }
}
