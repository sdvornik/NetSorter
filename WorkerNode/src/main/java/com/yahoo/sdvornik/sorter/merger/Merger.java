package com.yahoo.sdvornik.sorter.merger;

import com.yahoo.sdvornik.main.WorkerEntryPoint;
import com.yahoo.sdvornik.message.DataMessageArray;
import com.yahoo.sdvornik.message.Message;
import com.yahoo.sdvornik.Constants;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Merging sorted long array implementation. For better perfomance
 * all arrays length must be almost equal. This instance holds merging
 * result until master node request it.
 */
public enum Merger {

    INSTANCE;

    private final Logger log = LoggerFactory.getLogger(Merger.class);

    private final ExecutorService executor = Executors.newFixedThreadPool(1);

    private final LinkedBlockingQueue<long[]> queue = new LinkedBlockingQueue<>();

    private final Map<Integer, long[]> storage = new HashMap<>();

    private int maxTaskNumber;

    private long[] result;

    private final Runnable mergeTask = new Runnable() {

        @Override
        public void run() {
            int currentTaskNumber = 0;
            while(currentTaskNumber < maxTaskNumber) {
                try {
                    long[] arr= queue.take();
                    ++currentTaskNumber;

                    int stage = 0;
                    while(true) {
                        long[] savedArr = storage.get(stage);
                        if(savedArr == null) {
                            storage.put(stage, arr);
                            break;
                        }
                        else {
                            storage.put(stage, null);
                            arr = merge(arr, savedArr);
                            ++stage;
                        }
                    }
                }
                catch(InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            result = storage.values().stream().filter(arr -> arr!=null).findFirst().get();
            storage.clear();
            queue.clear();
            maxTaskNumber = 0;
            log.info("Merging successfully ended. Resulting array length: "+result.length+"; first: "+result[0]+"; last: "+result[result.length-1]);

            Channel masterChannel = WorkerEntryPoint.INSTANCE.getMasterNodeChannel();
            if(masterChannel != null) {
                masterChannel.writeAndFlush(
                        Message.getSimpleOutboundMessage(Message.Type.JOB_ENDED)
                );
            }
            else {
                result = null;
            }
        }
    };

    /**
     * Unblocking add array in {@link LinkedBlockingQueue}
     * for further processing.
     * @param arr
     * @return
     */
    public boolean putArrayInQueue(long[] arr) {
        return queue.add(arr);
    }

    public void init(int maxTaskNumber) {
        this.maxTaskNumber = maxTaskNumber;
        executor.execute(mergeTask);
    }

    /**
     * Send sorted array to master node.
     * Size of chunk is determined by {@code Constants.RESULT_CHUNK_SIZE_IN_KEYS}
     * @throws Exception
     */
    public void sendResult() throws Exception {
        int totalNumberOfChunk = result.length/Constants.RESULT_CHUNK_SIZE_IN_KEYS +
                ((result.length%Constants.RESULT_CHUNK_SIZE_IN_KEYS == 0) ? 0 : 1);

        for(int numberOfChunk = 0; numberOfChunk < totalNumberOfChunk; ++numberOfChunk) {

            int count = (numberOfChunk < totalNumberOfChunk-1) ?
                    Constants.RESULT_CHUNK_SIZE_IN_KEYS :
                    (result.length - numberOfChunk * Constants.RESULT_CHUNK_SIZE_IN_KEYS);


            DataMessageArray msg = new DataMessageArray(
                    Arrays.copyOfRange(
                            result,
                            numberOfChunk * Constants.RESULT_CHUNK_SIZE_IN_KEYS,
                            numberOfChunk * Constants.RESULT_CHUNK_SIZE_IN_KEYS+count
                    ),
                    numberOfChunk
            );

            WorkerEntryPoint.INSTANCE.getMasterNodeChannel().writeAndFlush(msg);

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
}
