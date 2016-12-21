package com.yahoo.sdvornik.merger;

import com.yahoo.sdvornik.master.MasterTask;
import com.yahoo.sdvornik.sharable.Constants;
import fj.Unit;
import io.netty.channel.Channel;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

public final class Merger {

    private final static Logger log = LoggerFactory.getLogger(MasterTask.class.getName());


    private final Map<String, LinkedBlockingDeque<long[]>> dequeMap = new HashMap<>();
    private final ExecutorService executor = Executors.newFixedThreadPool(1);

    private final fj.data.List<String> idList;
    private final long numberOfKeys;

    private final fj.F0<Unit> before;
    private final fj.F<String, Unit> onError;
    private final fj.F0<Unit> onSuccess;

    public Merger(
            fj.data.List<String> idList,
            long numberOfKeys,
            fj.F0<Unit> before,
            fj.F<String, Unit> onError,
            fj.F0<Unit> onSuccess
    ) {
        this.idList = idList;
        this.numberOfKeys = numberOfKeys;
        this.before = before;
        this.onError = onError;
        this.onSuccess = onSuccess;
    }

    private Runnable mergeTask = new Runnable() {

        @Override
        public void run() {

            if(before!=null) before.f();
            int totalKeysInOneGeneration = Constants.RESULT_CHUNK_SIZE_IN_KEYS*idList.length();
            int maxGeneration = ((int)numberOfKeys/totalKeysInOneGeneration +
                    (numberOfKeys%totalKeysInOneGeneration == 0 ? 0:1));
            int i = 0;

            try {
                multiMergeAndSave(idList.array(String[].class),  maxGeneration, totalKeysInOneGeneration);
                if(onSuccess!=null) onSuccess.f();
            }
            catch(InterruptedException e) {
                if(onError!=null) onError.f("Unexpected InterruptedException");
            }
            dequeMap.clear();
        }
    };

    public void init() {
        idList.foreach(
                id -> {
                    dequeMap.put(id, new LinkedBlockingDeque<long[]>());
                    return Unit.unit();
                }
        );
        executor.execute(mergeTask);
        log.info("Init Merger");
    }

    public void shutdownNow() {
        dequeMap.clear();
        executor.shutdownNow();
    }

    public void multiMergeAndSave(String[] id, int maxGeneration, int totalKeysInOneGeneration) throws InterruptedException {

        long[][] multiArr = new long[id.length][];
        int[] curIndex = new int[id.length];
        int[] curGeneration = new int[id.length];

        int curNumberOfArrWithMinValue = -1;
        int generation = 0;

        for(int i = 0; i< id.length; ++i) {
            multiArr[i] = dequeMap.get(id[i]).takeFirst();
        }

        System.out.println("MaxGeneration: "+maxGeneration);
        while(generation < maxGeneration) {
            int mergeArrLength = (int)(generation < maxGeneration - 1 ?
                    totalKeysInOneGeneration : numberOfKeys - totalKeysInOneGeneration*generation);
            long[] mergeArr = new long[mergeArrLength];
            for (int i = 0; i < mergeArrLength; ++i) {
                long curMinValue = Long.MAX_VALUE;
                int indexOfArrWithMinValue = 0;
                for (int k = 0; k < multiArr.length; ++k) {
                    if (curIndex[k] < multiArr[k].length && multiArr[k][curIndex[k]] < curMinValue) {
                        curMinValue = multiArr[k][curIndex[k]];
                        curNumberOfArrWithMinValue = k;
                    }
                }
                ++curIndex[curNumberOfArrWithMinValue];
                mergeArr[i] = curMinValue;

                if (curIndex[curNumberOfArrWithMinValue] == multiArr[curNumberOfArrWithMinValue].length) {

                    if(curGeneration[curNumberOfArrWithMinValue]<maxGeneration-1) {

                        LinkedBlockingDeque<long[]> deque = dequeMap.get(id[curNumberOfArrWithMinValue]);
                        log.info("Try to get element from deque: "+deque.size()+"; Generation: "+curGeneration[curNumberOfArrWithMinValue]);
                        multiArr[curNumberOfArrWithMinValue] = deque.takeFirst();


                        curIndex[curNumberOfArrWithMinValue] = 0;
                        ++curGeneration[curNumberOfArrWithMinValue];
                    }
                }
            }
            //System.out.println("First: "+mergeArr[0]+"; Last: "+mergeArr[mergeArr.length-1]+"; Length: "+mergeArr.length+"; Generation: "+generation);
            //TODO Save file
            ++generation;
        }

        log.info("End of merging");
    }

    public void putArrayInQueue(String id, int numberOfChunk, long[] sortedArr) {
        LinkedBlockingDeque<long[]> deque = dequeMap.get(id);
        log.info("addLast in queue chunk: "+numberOfChunk+"; queueSize: "+deque.size());
        deque.addLast(sortedArr);
    }
}
