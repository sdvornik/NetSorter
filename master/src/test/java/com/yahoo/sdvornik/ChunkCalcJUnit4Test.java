package com.yahoo.sdvornik;

import com.yahoo.sdvornik.master.MasterTask;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;

public class ChunkCalcJUnit4Test {

    private final static int FILE_SIZE_IN_MB = 1057;

    @Test
    public void testCalcChunkSize() throws Exception{
        int countOfWorkerNodes = 9;
        long numberOfKeys = FILE_SIZE_IN_MB* Constants.BYTES_IN_MBYTES/Long.BYTES;

        Method method = MasterTask.class.getDeclaredMethod("calcChunkQuantity", long.class, int.class);
        method.setAccessible(true);
        int totalChunkQuantity = (int)method.invoke(MasterTask.INSTANCE, numberOfKeys, countOfWorkerNodes);

        System.out.println("totalChunkQuantity: "+totalChunkQuantity);

        int totalChunkQuantityToOneNodeModule = totalChunkQuantity%countOfWorkerNodes;
        Assert.assertTrue(totalChunkQuantityToOneNodeModule == 0);

        int totalChunkQuantityToOneNode = totalChunkQuantity/countOfWorkerNodes;
        System.out.println("totalChunkQuantityToOneNode: "+totalChunkQuantityToOneNode);

        double ln = Math.log(totalChunkQuantityToOneNode)/Math.log(2);
        Assert.assertTrue(Math.abs(ln-Math.ceil(ln))==0);

        int chunkSize = (int)Math.ceil(numberOfKeys/(double)totalChunkQuantity);
        System.out.println("ChunkSize: "+chunkSize + " vs default: "+Constants.DEFAULT_CHUNK_SIZE_IN_KEYS);

        Assert.assertTrue(chunkSize*totalChunkQuantity - numberOfKeys < chunkSize);

    }
}


