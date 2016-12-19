package com.yahoo.sdvornik.master;

import com.yahoo.sdvornik.merger.Merger;
import com.yahoo.sdvornik.sharable.Constants;
import com.yahoo.sdvornik.main.EntryPoint;
import com.yahoo.sdvornik.sharable.MasterWorkerMessage;
import fj.Try;
import fj.Unit;
import fj.data.Validation;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
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
    private fj.data.List<Channel> channelList = fj.data.List.nil();
    private fj.data.List<Boolean> responseList = fj.data.List.nil();
    private final ExecutorService executor = Executors.newFixedThreadPool(1);
    private Map<String, LinkedBlockingDeque<long[]>> dequeMap = new HashMap<>();

    public Validation<? extends Exception, Unit> distributeTask(Path pathToFile) {
        return Try.f(
                () -> {
                    channelList = fj.data.List.iterableList(EntryPoint.getMasterChannelGroup());
                    if (channelList.isEmpty()) {
                        throw new IllegalStateException("Nothing worker node are connected to master node");
                    }

                    boolean isLocked = !lock.compareAndSet(0, 1);
                    if (isLocked) {
                        throw new IllegalStateException("Operation is locked");
                    }

                    try (SeekableByteChannel seekableByteChannel =
                                Files.newByteChannel(pathToFile, EnumSet.of(StandardOpenOption.READ))) {

                        long numberOfKeys = seekableByteChannel.size() / Long.BYTES;
                        log.info("Total number of keys: "+numberOfKeys);
                        int countOfWorkerNodes = channelList.length();
                        int totalChunkQuantity = calcChunkQuantity(numberOfKeys, countOfWorkerNodes);
                        int chunkQuantityToOneNode = totalChunkQuantity / countOfWorkerNodes;

                        int chunkSize = (int) Math.ceil(numberOfKeys / (double) totalChunkQuantity);
                        log.info("chunkSize: "+chunkSize);

                        log.info("Total number of chunks for one node " + chunkQuantityToOneNode);

                        MasterWorkerMessage enumMsg = MasterWorkerMessage.START_SORTING;
                        enumMsg.setIntPayload(chunkQuantityToOneNode);

                        for(Channel channel : channelList) {
                              channel.writeAndFlush(enumMsg.getByteBuf()).sync();
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
                            log.info("Successfully send stop msg");
                        }

                    }
                    catch(Exception e) {
                        channelList = fj.data.List.nil();
                        responseList = fj.data.List.nil();
                        boolean isUnlocked = lock.compareAndSet(1,0);
                        if(!isUnlocked) {
                            IllegalStateException ex = new IllegalStateException("Can't unlock locked file");
                            ex.initCause(e);
                            throw ex;
                        }
                        throw e;
                    }
                    return Unit.unit();
                }
        ).f();
    }

    public void saveResponse() {
        responseList = fj.data.List.cons(Boolean.TRUE, responseList);
        if(responseList.length() == channelList.length()) {
            collectResult();
        }
        //TODO set timeout for cancel operation
    }

    private Validation<? extends Exception, Unit> collectResult() {

        return Try.f( () ->
                {
                    initMerger();
                    for (Channel channel : channelList) {
                        channel.writeAndFlush(MasterWorkerMessage.GET_RESULT.getByteBuf()).sync();
                        log.info("Successfully send collect msg");
                    }

                    return Unit.unit();
                }
        ).f();
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
                long[][] multiArr = new long[dequeMap.size()][];
                String[] id = new String[dequeMap.size()];
                int i = 0;
                try {
                    for (Map.Entry<String,LinkedBlockingDeque<long[]>> dequeEntrySet : dequeMap.entrySet()) {
                        id[i] = dequeEntrySet.getKey();
                        multiArr[i++] = dequeEntrySet.getValue().takeFirst();
                    }
                }
                catch(InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                multiMerge(multiArr, new long[20], id);
            }
        });
        log.info("Init Merger");
    }

    public void multiMerge(long[][] multiArr, long[] mergeArr, String[] id) {
        int mergeArrLength = mergeArr.length;

        int[] curIndex = new int[multiArr.length];
        long curMinValue = Long.MAX_VALUE;
        int curNumberOfArrWithMinValue = -1;
        int firstIndex=0;
        int secondIndex=0;

        for (int i = 0; i < mergeArrLength; ++i) {
            int indexOfArrWithMinValue = 0;
            for(int k = 0; k < multiArr.length; ++k) {
                if(curIndex[k] < multiArr[k].length && multiArr[k][curIndex[k]] < curMinValue) {
                    curMinValue = multiArr[k][curIndex[k]];
                    curNumberOfArrWithMinValue = k;
                }
            }
            ++curIndex[curNumberOfArrWithMinValue];
            mergeArr[i] = curMinValue;
            if(curIndex[curNumberOfArrWithMinValue] == multiArr[curNumberOfArrWithMinValue].length) {
                try {
                    multiArr[curNumberOfArrWithMinValue] = dequeMap.get(id[curNumberOfArrWithMinValue]).takeFirst();
                    curIndex[curNumberOfArrWithMinValue]=0;
                }
                catch(Exception e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

    }

    public void putArrayInQueue(String id, int numberOfChunk, long[] sortedArr) {
        LinkedBlockingDeque<long[]> deque = dequeMap.get(id);
        deque.addLast(sortedArr);
        log.info("id "+id+"; chunk "+numberOfChunk+"; length: "+sortedArr.length);
    }

    private int calcChunkQuantity(long numberOfKeys, int countOfWorkerNodes) {
        double parameter = (double) (countOfWorkerNodes*Constants.DEFAULT_CHUNK_SIZE_IN_KEYS);
        int power = (int)Math.floor(Math.log(numberOfKeys/parameter)/Math.log(2));

        return countOfWorkerNodes*(int)Math.pow(2,power);
    }
}
