package com.yahoo.sdvornik.master;

import com.yahoo.sdvornik.message.Message;
import com.yahoo.sdvornik.message.DataMessageBuffer;
import com.yahoo.sdvornik.message.StartSortingMessage;
import com.yahoo.sdvornik.Constants;
import com.yahoo.sdvornik.main.MasterEntryPoint;import fj.Unit;
import io.netty.channel.*;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public enum MasterTask {

    INSTANCE;

    private final Logger log = LoggerFactory.getLogger(MasterTask.class);
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
        this.channelList = fj.data.List.iterableList(MasterEntryPoint.INSTANCE.getMasterChannelGroup());
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

    public void distributeTask() throws Exception {
        try (SeekableByteChannel seekableByteChannel =
                                Files.newByteChannel(pathToFile, EnumSet.of(StandardOpenOption.READ))) {

            int countOfWorkerNodes = channelList.length();

            numberOfKeys = seekableByteChannel.size() / Long.BYTES;
            log.info("Total number of keys: "+numberOfKeys);

            int totalChunkQuantity = calcChunkQuantity(numberOfKeys, countOfWorkerNodes);
            int chunkQuantityToOneNode = totalChunkQuantity / countOfWorkerNodes;
            log.info("Total number of chunks for one node " + chunkQuantityToOneNode);

            int chunkSize = (int) Math.ceil(numberOfKeys / (double) totalChunkQuantity);
            log.info("ChunkSize: "+chunkSize);

            Message msg = new StartSortingMessage(chunkQuantityToOneNode);
            for(Channel outputChannel : channelList) {
                    outputChannel.writeAndFlush(msg);
            }

            for (int numberOfCycle = 0 ; numberOfCycle < chunkQuantityToOneNode; ++numberOfCycle) {
                int numberOfNode = 0;
                for (Channel outputChannel : channelList) {
                    ByteBuffer buffer = ByteBuffer.allocate(chunkSize*Long.BYTES);

                    int numberOfChunk = numberOfCycle * countOfWorkerNodes + numberOfNode;

                    seekableByteChannel.position(numberOfChunk*chunkSize * Long.BYTES);
                    seekableByteChannel.read(buffer);
                    buffer.flip();
                    DataMessageBuffer dataMsg = new DataMessageBuffer(buffer, numberOfChunk);

                    outputChannel.writeAndFlush(dataMsg);
                    ++numberOfNode;
                }
            }

            for(Channel channel : channelList) {
                channel.writeAndFlush(
                        Message.getSimpleOutboundMessage(Message.Type.TASK_TRANSMISSION_ENDED)
                );
            }
            log.info("Successfully send TASK_TRANSMISSION_ENDED message");
        }
        //TODO set timeout for cancel operation
    }

    public void saveResponse(String id) {
        wsClientChannel.writeAndFlush(new TextWebSocketFrame("Worker with id "+id+" finished job"));
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
        Path pathToResultFile = Paths.get(System.getProperty("user.home"), Constants.OUTPUT_FILE_NAME);
        try {
            Files.deleteIfExists(pathToResultFile);
            Files.createFile(pathToResultFile);
        }
        catch(Exception e) {
            log.error("Can't create output file.");
            throw new RuntimeException(e);
        }

        mergerInstance = new Merger(
                idList,
                numberOfKeys,
                Constants.RESULT_CHUNK_SIZE_IN_KEYS*idList.length(),
                pathToResultFile,
                before,
                onError,
                onSuccess
        );
        mergerInstance.init();
        try {
            for (Channel channel : channelList) {
                channel.writeAndFlush(Message.getSimpleOutboundMessage(Message.Type.GET_RESULT)).sync();
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
