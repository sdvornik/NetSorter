package com.yahoo.sdvornik.sorter;

/**
 * Implementation of BubbleSort algorithm.
 */
public final class BubbleSort extends Sorter {
    /**
     * Ctor.
     * @param longArr
     */
    public BubbleSort(long[] longArr) {
        super(longArr);
    }

    public long[] sort() {
		for (int i = 0; i < longArr.length; ++i) {
			for (int j = 0; j < longArr.length - i - 1; ++j) {
				if (longArr[j] > longArr[j + 1]) swap(j,j+1);
	        }
		}
		return longArr;
	}
}
