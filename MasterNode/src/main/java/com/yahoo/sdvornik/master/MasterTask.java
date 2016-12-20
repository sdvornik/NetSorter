package com.yahoo.sdvornik.master;

import com.yahoo.sdvornik.sharable.Constants;
import com.yahoo.sdvornik.main.Master;
import com.yahoo.sdvornik.sharable.MasterWorkerMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

public enum MasterTask {

    INSTANCE;

    private final Logger log = LoggerFactory.getLogger(MasterTask.class.getName());
    private AtomicInteger lock = new AtomicInteger(0);
    private Path pathToFile;
    private Channel wsClientChannel;
    private fj.data.List<Channel> channelList = fj.data.List.nil();


    private long numberOfKeys;

    private fj.data.List<Boolean> responseList = fj.data.List.nil();
    private final ExecutorService executor = Executors.newFixedThreadPool(1);
    private Map<String, LinkedBlockingDeque<long[]>> dequeMap = new HashMap<>();

    public void runTask(Path pathToFile, Channel wsClientChannel) throws Exception {
        this.pathToFile = pathToFile;
        this.wsClientChannel = wsClientChannel;
        this.channelList = fj.data.List.iterableList(Master.INSTANCE.getMasterChannelGroup());
        if (channelList.isEmpty()) {
            throw new IllegalStateException("Nothing worker node are connected to master node");
        }
        boolean isLocked = !lock.compareAndSet(0, 1);
        if (isLocked) {
            throw new IllegalStateException("Operation is locked");
        }
        try {
            distributeTask();
            wsClientChannel.writeAndFlush(new TextWebSocketFrame("Succesfully send task to worker node"));
        }
        catch(Exception e) {
            boolean isUnlocked = lock.compareAndSet(1,0);
            if(!isUnlocked) {
                IllegalStateException ex = new IllegalStateException("Can't unlock locked file");
                ex.initCause(e);
                throw ex;
            }
            throw e;
        }
    }

    public void distributeTask() throws Exception {
        try (SeekableByteChannel seekableByteChannel =
                                Files.newByteChannel(pathToFile, EnumSet.of(StandardOpenOption.READ))) {

            numberOfKeys = seekableByteChannel.size() / Long.BYTES;
            log.info("Total number of keys: "+numberOfKeys);
            int countOfWorkerNodes = channelList.length();
            int totalChunkQuantity = calcChunkQuantity(numberOfKeys, countOfWorkerNodes);
            int chunkQuantityToOneNode = totalChunkQuantity / countOfWorkerNodes;

            int chunkSize = (int) Math.ceil(numberOfKeys / (double) totalChunkQuantity);
            log.info("ChunkSize: "+chunkSize);

            log.info("Total number of chunks for one node " + chunkQuantityToOneNode);

            MasterWorkerMessage enumMsg = MasterWorkerMessage.START_SORTING;
            ByteBuf buf = enumMsg.getByteBuf(chunkQuantityToOneNode);
            for(Channel channel : channelList) {
                  channel.writeAndFlush(buf).sync();
            }

            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES+Integer.BYTES+chunkSize*Long.BYTES);

            for (int numberOfCycle = 0; numberOfCycle < chunkQuantityToOneNode; ++numberOfCycle) {
                int numberOfNode = 0;
                for (Channel outputChannel : channelList) {
                    int numberOfChunk = numberOfCycle * countOfWorkerNodes + numberOfNode;

                    long count = numberOfChunk < totalChunkQuantity-1 ?
                            chunkSize * Long.BYTES :
                            seekableByteChannel.size() - numberOfChunk * chunkSize * Long.BYTES;
                    buffer.putLong(Integer.BYTES+count);
                    buffer.putInt(numberOfChunk);
                    seekableByteChannel.position(numberOfChunk*chunkSize * Long.BYTES);
                    seekableByteChannel.read(buffer);
                    buffer.flip();
                    ByteBuf nettyBuf = Unpooled.wrappedBuffer(buffer);
                    outputChannel.writeAndFlush(nettyBuf).sync();
                    buffer.clear();
                    ++numberOfNode;
                }
            }

            for(Channel channel : channelList) {
                channel.writeAndFlush(MasterWorkerMessage.STOP_TASK_TRANSMISSION.getByteBuf()).sync();
            }
            log.info("Successfully send stop transmission message");
        }
        //TODO set timeout for cancel operation
    }

    public void saveResponse(String id) {
        wsClientChannel.writeAndFlush(new TextWebSocketFrame("Worker with id "+id+"finished job"));
        responseList = fj.data.List.cons(Boolean.TRUE, responseList);
        if(responseList.length() == channelList.length()) {
            collectResult();
        }
        //TODO set timeout for cancel operation
    }

    private  void collectResult() {

        initMerger();
        try {
            for (Channel channel : channelList) {
                channel.writeAndFlush(MasterWorkerMessage.GET_RESULT.getByteBuf()).sync();
            }
            String message = "Successfully send collect message";
            wsClientChannel.writeAndFlush(new TextWebSocketFrame(message));
            log.info(message);
        }
        catch(Exception e) {
            String errorMessage = "Can't send collect message to worker nodes. Task execution stopped.";
            wsClientChannel.writeAndFlush(new TextWebSocketFrame(errorMessage));
            log.info(errorMessage);
            boolean isUnlocked = lock.compareAndSet(1,0);
            if(!isUnlocked) {
                throw new RuntimeException();
            }
            stopMerger();
        }
    }

    private void stopMerger() {
        dequeMap.clear();
        executor.shutdownNow();
    }

    private void initMerger() {
        for (Channel channel : channelList) {
            dequeMap.put(
                    channel.id().asShortText(),
                    new LinkedBlockingDeque<long[]>()
            );
        }
        executor.execute(new Runnable() {

            @Override
            public void run() {
                String message = "Start to merge worker nodes results.";
                wsClientChannel.writeAndFlush(new TextWebSocketFrame(message));
                log.info(message);

                String[] id = new String[channelList.length()];
                int totalKeysInOneGeneration = Constants.RESULT_CHUNK_SIZE_IN_KEYS*channelList.length();
                int maxGeneration = ((int)numberOfKeys/totalKeysInOneGeneration +
                        (numberOfKeys%totalKeysInOneGeneration == 0 ? 0:1));
                int i = 0;

                for (Map.Entry<String,LinkedBlockingDeque<long[]>> dequeEntrySet : dequeMap.entrySet()) {
                    id[i] = dequeEntrySet.getKey();
                }

                try {
                    multiMergeAndSave(id,  maxGeneration, totalKeysInOneGeneration);
                }
                catch(InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                dequeMap.clear();
                boolean isUnlocked = lock.compareAndSet(1,0);
                if(!isUnlocked) {
                    throw new RuntimeException();
                }
                message = "Successfully merged worker nodes results.";
                wsClientChannel.writeAndFlush(new TextWebSocketFrame(message));
                log.info(message);
            }
        });
        log.info("Init Merger");
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

    private int calcChunkQuantity(long numberOfKeys, int countOfWorkerNodes) {
        double parameter = (double) (countOfWorkerNodes*Constants.DEFAULT_CHUNK_SIZE_IN_KEYS);
        int power = (int)Math.floor(Math.log(numberOfKeys/parameter)/Math.log(2));

        return countOfWorkerNodes*(int)Math.pow(2,power);
    }
}
