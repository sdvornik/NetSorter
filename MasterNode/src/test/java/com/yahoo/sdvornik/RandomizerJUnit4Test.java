package com.yahoo.sdvornik;

import com.yahoo.sdvornik.master.MasterTask;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.stream.Collectors;

public class RandomizerJUnit4Test extends Assert {

    private final static int LENGTH = 1000;

    @Test
    public void testIndexRandomizer() throws Exception {

        Method method = MasterTask.class.getDeclaredMethod("arrayIndexRandomizer", int.class);
        method.setAccessible(true);

        long start = System.nanoTime();
        int[] res = (int[])method.invoke(MasterTask.INSTANCE, LENGTH);
        long end = System.nanoTime();

        Arrays.stream(res)
                .boxed()
                .collect(Collectors.toList());
        Integer sum = fj.data.List.iterableList(Arrays.stream(res)
                .boxed()
                .collect(Collectors.toList())
        ).foldLeft((Integer elm, Integer acc)-> elm+acc, 0);
        System.out.println("Generate random array mapping: "+(end-start)/1000000+" ms");
        Assert.assertTrue(sum == (LENGTH-1)*LENGTH/2 && (LENGTH-1)*LENGTH%2==0);
    }

    @Test
    public void byteBufferRandomizer() throws Exception {

        Method indexRandomizerMethod = MasterTask.class.getDeclaredMethod("arrayIndexRandomizer", int.class);
        indexRandomizerMethod.setAccessible(true);

        int[] res = (int[])indexRandomizerMethod.invoke(MasterTask.INSTANCE, LENGTH);

        Method byteBufferRandomizerMethod =
                MasterTask.class.getDeclaredMethod("byteBufferRandomizer", ByteBuffer.class, int.class, int.class, int[].class);
        byteBufferRandomizerMethod.setAccessible(true);

        ByteBuffer buffer = ByteBuffer.allocate(LENGTH*Long.BYTES);

        for(int i = 0; i< LENGTH; ++i) {
            buffer.putLong(i);
        }

        long start = System.nanoTime();
        byteBufferRandomizerMethod.invoke(MasterTask.INSTANCE, buffer, 0, LENGTH*Long.BYTES, res);
        long end = System.nanoTime();

        int sum = 0;
        for(int i = 0; i< LENGTH; ++i) {
            sum+=buffer.getLong();
        }
        System.out.println("Shuffle buffer: "+(end-start)/1000000+" ms");
        Assert.assertTrue(sum == (LENGTH-1)*LENGTH/2 && (LENGTH-1)*LENGTH%2==0);
    }

}
