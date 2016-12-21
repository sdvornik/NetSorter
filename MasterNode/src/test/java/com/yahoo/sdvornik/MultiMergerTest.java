package com.yahoo.sdvornik;

import com.yahoo.sdvornik.merger.Merger;
import fj.Unit;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;

public class MultiMergerTest {
    @Test
    public void testMultiMerger() throws Exception {
        fj.data.List<String> idList = fj.data.List.list("id1", "id2", "id3");
        long numberOfKeys = 3000;

        Merger merger =  new Merger(idList, numberOfKeys, null, null, null);

/*

        Method method = Merger.class.getDeclaredMethod("multiMerge", long[][].class);
        method.setAccessible(true);

        long start = System.nanoTime();
        long[] resArray = Merger.INSTANCE.multiMerge(multiArray);
        long end = System.nanoTime();
        System.out.println("MultiMerger : "+(end-start)/1000000+" ms");
        Assert.assertTrue(checkSortedArray(resArray));
        */
    }
}
