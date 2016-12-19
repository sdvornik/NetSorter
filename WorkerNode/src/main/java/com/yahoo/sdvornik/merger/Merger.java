package com.yahoo.sdvornik.merger;

import com.yahoo.sdvornik.main.EntryPoint;
import com.yahoo.sdvornik.sharable.Constants;
import com.yahoo.sdvornik.sharable.MasterWorkerMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public enum Merger {

    INSTANCE;

    private final Logger log = LoggerFactory.getLogger(Merger.class.getName());

    private final ExecutorService executor = Executors.newFixedThreadPool(1);

    private final LinkedBlockingQueue<long[]> queue = new LinkedBlockingQueue<>();

    private final Map<Integer, long[]> storage = new HashMap<>();

    private int currentTaskNumber = 0;

    private int maxTaskNumber = -1;

    private long[] result;

    private final Runnable mergeTask = new Runnable() {

        @Override
        public void run() {
            while(currentTaskNumber < maxTaskNumber) {
                try {
                    long[] arr= queue.take();
                    ++currentTaskNumber;

                    int stage = 0;
                    while(true) {
                        long[] savedArr = storage.get(stage);
                        if(savedArr == null) {
                            log.info("put array with length "+arr.length);
                            storage.put(stage, arr);
                            break;
                        }
                        else {
                            storage.put(stage, null);
                            arr = merge(arr, savedArr);
                            log.info("successfully merge "+arr.length);
                            ++stage;
                        }
                    }
                }
                catch(InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            maxTaskNumber = -1;
            currentTaskNumber = 0;
            result = storage.values().stream().filter(arr -> arr!=null).findFirst().get();
            storage.clear();
            log.info("Merging successfully ended. Resulting array length: "+result.length);

            Channel masterChannel = EntryPoint.getMasterNodeChannel();
            if(masterChannel != null) {
                masterChannel.writeAndFlush(MasterWorkerMessage.JOB_ENDED.getByteBuf());
            }
            else {
                result = null;
            }
        }
    };

    public boolean putArrayInQueue(long[] arr) {
        return queue.add(arr);
    }

    public void init(int maxTaskNumber) {
        this.maxTaskNumber = maxTaskNumber;
        this.currentTaskNumber = 0;
        executor.execute(mergeTask);
    }

    public long[] getResult() {
        if(result == null) throw new IllegalStateException("Merger not contain result");
        long[] tmp = result;
        result = null;
        return tmp;
    }

    public void sendResult() throws Exception {
        int totalNumberOfChunk = result.length/Constants.RESULT_CHUNK_SIZE_IN_KEYS +
                ((result.length%Constants.RESULT_CHUNK_SIZE_IN_KEYS == 0) ? 0 : 1);

        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES+Integer.BYTES+Constants.RESULT_CHUNK_SIZE_IN_KEYS *Long.BYTES);

        for(int numberOfChunk = 0; numberOfChunk < totalNumberOfChunk; ++numberOfChunk) {

            long count = (numberOfChunk < totalNumberOfChunk-1) ?
                    Constants.RESULT_CHUNK_SIZE_IN_KEYS :
                    (result.length - numberOfChunk * Constants.RESULT_CHUNK_SIZE_IN_KEYS);
            buffer.putLong(Integer.BYTES+count*Long.BYTES);
            buffer.putInt(numberOfChunk);
            for(int i = numberOfChunk; i < numberOfChunk + count; ++i) {
                buffer.putLong(result[i]);
            }
            buffer.flip();
            ByteBuf nettyBuf = Unpooled.wrappedBuffer(buffer);
            EntryPoint.getMasterNodeChannel().writeAndFlush(nettyBuf);
            buffer.clear();
        }
        log.info("Successfully send result");
        result = null;
    }

    public void stop() {
        executor.shutdownNow();
    }

    private long[] merge(long[] firstArr, long[] secondArr) {
        long[] resArr = new long[firstArr.length+secondArr.length];
        int firstIndex=0;
        int secondIndex=0;

        for (int i = 0; i < resArr.length; ++i) {
            if (firstIndex < firstArr.length && secondIndex < secondArr.length) {
                resArr[i] = (firstArr[firstIndex] < secondArr[secondIndex]) ?
                        firstArr[firstIndex++] : secondArr[secondIndex++];
            }
            else if (firstIndex < firstArr.length) {
                resArr[i] = firstArr[firstIndex++];
            }
            else {
                resArr[i] = secondArr[secondIndex++];
            }
        }
        return resArr;
    }

    //TODO Remove
    public long[] multiMerge(long[][] arr) {
        int mergeArrLength = 0;
        for(int numberOfArr = 0; numberOfArr < arr.length; ++numberOfArr) {
            mergeArrLength+= arr[numberOfArr].length;
        }
        long[] mergeArr = new long[mergeArrLength];
        int[] curIndex = new int[arr.length];
        long curMinValue = Long.MAX_VALUE;
        int curNumberOfArrWithMinValue = -1;
        int firstIndex=0;
        int secondIndex=0;

        for (int i = 0; i < mergeArrLength; ++i) {
            int indexOfArrWithMinValue = 0;
            for(int k = 0; k < arr.length; ++k) {
                if(curIndex[k] < arr[k].length && arr[k][curIndex[k]] < curMinValue) {
                    curMinValue = arr[k][curIndex[k]];
                    curNumberOfArrWithMinValue = k;
                }
            }
            ++curIndex[curNumberOfArrWithMinValue];
            mergeArr[i] = curMinValue;
        }
        return mergeArr;
    }
}
