package com.yahoo.sdvornik.sorter;

public final class InsertionSort extends Sorter {


    public InsertionSort(long[] longArr) {
        super(longArr);
    }

    public long[] sort(){
        for(int j = 1; j< longArr.length; ++j){
            long key = longArr[j];
            int i = j - 1;
            while (i >= 0 && longArr[i] > key){
                longArr[i + 1] = longArr[i];
                --i;
            }
            longArr[i + 1] = key;
        }
        return longArr;
    }
}