package com.yahoo.sdvornik.master;

import com.yahoo.sdvornik.merger.Merger;
import com.yahoo.sdvornik.sharable.Constants;
import com.yahoo.sdvornik.main.Master;
import com.yahoo.sdvornik.sharable.MasterWorkerMessage;
import fj.Unit;
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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public enum MasterTask {

    INSTANCE;

    private final Logger log = LoggerFactory.getLogger(MasterTask.class.getName());
    private final static ThreadLocalRandom random = ThreadLocalRandom.current();

    private AtomicInteger lock = new AtomicInteger(0);
    private Path pathToFile;
    private Channel wsClientChannel;

    private fj.data.List<Channel> channelList = fj.data.List.nil();
    private fj.data.List<Boolean> responseList = fj.data.List.nil();
    private Merger mergerInstance;

    private long numberOfKeys;

    fj.F0<Unit> before = new fj.F0<Unit>() {

        @Override
        public Unit f() {
            String message = "Start to merge worker nodes results.";
            wsClientChannel.writeAndFlush(new TextWebSocketFrame(message));
            log.info(message);
            return Unit.unit();
        }
    };

    fj.F0<Unit> onSuccess = new fj.F0<Unit>() {

        @Override
        public Unit f() {
            channelList = fj.data.List.nil();
            responseList = fj.data.List.nil();
            mergerInstance = null;

            boolean isUnlocked = lock.compareAndSet(1,0);
            if(!isUnlocked) {
                throw new RuntimeException();
            }
            String message = "Successfully merged worker nodes results.";
            wsClientChannel.writeAndFlush(new TextWebSocketFrame(message));
            log.info(message);
            return Unit.unit();
        }
    };

    fj.F<String,Unit> onError = new fj.F<String,Unit>() {

        @Override
        public Unit f(String message) {
            channelList = fj.data.List.nil();
            responseList = fj.data.List.nil();
            mergerInstance = null;

            boolean isUnlocked = lock.compareAndSet(1,0);
            if(!isUnlocked) {
                throw new RuntimeException();
            }
            wsClientChannel.writeAndFlush(new TextWebSocketFrame(message));
            log.info(message);
            return Unit.unit();
        }
    };

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
            onError.f(e.getMessage());
            throw e;
        }
    }

    private int[] arrayIndexRandomizer(int length) {
        int[] a = new int[length];
        for(int i = 0; i < length; ++i){
            a[i] = i;
        }
        for(int i = length-1; i>=0; --i) {
            int j = random.nextInt(i+1);
            int tmp = a[j];
            a[j] = a[i];
            a[i] = tmp;
        }
        return a;
    }

    private void byteBufferRandomizer(ByteBuffer buffer, int offset, int limit, int[] map) {
        int blocksCount = (limit-offset)/Long.BYTES;
        if(blocksCount > map.length) {
            throw new IllegalArgumentException("Limit value must be equal or less map.length");
        }
        for(int i = 0; i< blocksCount; ++i) {
            byte[] tempI = new byte[Long.BYTES];
            byte[] tempJ = new byte[Long.BYTES];
            buffer.position(offset+i*Long.BYTES);
            buffer.get(tempI);
            buffer.position(offset+map[i]*Long.BYTES);
            buffer.get(tempJ);

            buffer.position(offset+i*Long.BYTES);
            buffer.put(tempJ, 0, Long.BYTES);
            buffer.position(offset+map[i]*Long.BYTES);
            buffer.put(tempI, 0, Long.BYTES);
        }
        buffer.position(0);
        buffer.limit(limit);
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
            int[] randomizerMap = arrayIndexRandomizer(chunkSize);
            log.info("ChunkSize: "+chunkSize);

            log.info("Total number of chunks for one node " + chunkQuantityToOneNode);

            MasterWorkerMessage enumMsg = MasterWorkerMessage.START_SORTING;
            ByteBuf buf = enumMsg.getByteBuf(chunkQuantityToOneNode);
            for(Channel channel : channelList) {
                  channel.writeAndFlush(buf).sync();
            }

            for (int numberOfCycle = 0, numberOfNode = 0; numberOfCycle < chunkQuantityToOneNode; ++numberOfCycle) {

                for (Channel outputChannel : channelList) {
                    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES+Integer.BYTES+chunkSize*Long.BYTES);

                    int numberOfChunk = numberOfCycle * countOfWorkerNodes + numberOfNode;

                    long count = numberOfChunk < totalChunkQuantity-1 ?
                            chunkSize * Long.BYTES :
                            seekableByteChannel.size() - numberOfChunk * chunkSize * Long.BYTES;
                    buffer.putLong(Integer.BYTES+count);
                    buffer.putInt(numberOfChunk);
                    seekableByteChannel.position(numberOfChunk*chunkSize * Long.BYTES);
                    seekableByteChannel.read(buffer);
                    int offset = Long.BYTES+Integer.BYTES;
                    int limit = buffer.position();

                    if((limit - offset)/Long.BYTES == randomizerMap.length) {
                        byteBufferRandomizer(buffer, offset, limit, randomizerMap);
                    }
                    else {
                        int[] restrictedRandomizerMap = arrayIndexRandomizer((limit - offset)/Long.BYTES);
                        byteBufferRandomizer(buffer, offset, limit, restrictedRandomizerMap);
                    }
                    ByteBuf nettyBuf = Unpooled.wrappedBuffer(buffer);
                    outputChannel.writeAndFlush(nettyBuf);
                    ++numberOfNode;
                }
            }

            for(Channel channel : channelList) {
                channel.writeAndFlush(MasterWorkerMessage.STOP_TASK_TRANSMISSION.getByteBuf());
            }
            log.info("Successfully send STOP_TASK_TRANSMISSION message");
        }
        //TODO set timeout for cancel operation
    }

    public void saveResponse(String id) {
        wsClientChannel.writeAndFlush(new TextWebSocketFrame("Worker with id "+id+"finished job"));
        responseList = fj.data.List.cons(Boolean.TRUE, responseList);
        if(responseList.length() == channelList.length()) {
            String message = "Start to merge worker nodes results.";
            wsClientChannel.writeAndFlush(new TextWebSocketFrame(message));
            log.info(message);
            responseList = fj.data.List.nil();
            collectResult();
        }

        //TODO set timeout for cancel operation
    }

    private  void collectResult() {

        fj.data.List<String> idList = channelList.map(
             channel -> channel.id().asShortText()
        );
        mergerInstance = new Merger(idList, numberOfKeys, before, onError, onSuccess);
        mergerInstance.init();
        try {
            for (Channel channel : channelList) {
                channel.writeAndFlush(MasterWorkerMessage.GET_RESULT.getByteBuf()).sync();
            }
            String message = "Successfully send collect message";
            wsClientChannel.writeAndFlush(new TextWebSocketFrame(message));
            log.info(message);
        }
        catch(Exception e) {
            mergerInstance.shutdownNow();
            onError.f(e.getMessage());
        }
    }

    public void putArrayInQueue(String id, int numberOfChunk, long[] sortedArr) {
        mergerInstance.putArrayInQueue(id, numberOfChunk, sortedArr);
    }

    private int calcChunkQuantity(long numberOfKeys, int countOfWorkerNodes) {
        double parameter = (double) (countOfWorkerNodes*Constants.DEFAULT_CHUNK_SIZE_IN_KEYS);
        int power = (int)Math.floor(Math.log(numberOfKeys/parameter)/Math.log(2));

        return countOfWorkerNodes*(int)Math.pow(2,power);
    }
}
