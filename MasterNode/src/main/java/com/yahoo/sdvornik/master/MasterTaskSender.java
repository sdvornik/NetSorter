package com.yahoo.sdvornik.master;

import com.yahoo.sdvornik.Constants;
import com.yahoo.sdvornik.main.EntryPoint;
import fj.Try;
import fj.Unit;
import fj.data.Validation;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicInteger;

public class MasterTaskSender {

    private static final Logger log = LoggerFactory.getLogger(MasterTaskSender.class.getName());
    private AtomicInteger lock = new AtomicInteger(0);

    private Path pathToFile;
    public MasterTaskSender(Path pathToFile) {
        this.pathToFile = pathToFile;
    }

    public static int calcChunkQuantity(long numberOfKeys, int countOfWorkerNodes) {
        double parameter = (double) (countOfWorkerNodes*Constants.DEFAULT_CHUNK_SIZE_IN_KEYS);
        int power = (int)Math.floor(Math.log(numberOfKeys/parameter)/Math.log(2));

        return countOfWorkerNodes*(int)Math.pow(2,power);
    }

    public Validation<? extends Exception, Unit> distributeTask() {
        return Try.f(
                () -> {
                    fj.data.List<Channel> channelList = EntryPoint.getMasterChannelList();
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
                        int countOfWorkerNodes = channelList.length();
                        int totalChunkQuantity = calcChunkQuantity(numberOfKeys, countOfWorkerNodes);
                        int chunkQuantityToOneNode = totalChunkQuantity / countOfWorkerNodes;

                        int chunkSize = (int) Math.ceil(numberOfKeys / (double) totalChunkQuantity);
                        log.info("chunkSize: "+chunkSize);

                        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES+Integer.BYTES+chunkSize*Long.BYTES);
                        log.info("Msg size in bytes "+(Long.BYTES+Integer.BYTES+chunkSize*Long.BYTES+Integer.BYTES));
                        log.info(" Total number of cycles " + chunkQuantityToOneNode);
                        for (int numberOfCycle = 0; numberOfCycle < chunkQuantityToOneNode; ++numberOfCycle) {

                            int numberOfNode = 0;
                            for (Channel outputChannel : channelList) {
                                int numberOfChunk = numberOfCycle * countOfWorkerNodes + numberOfNode;

                                long count = numberOfChunk < totalChunkQuantity-1 ?
                                        chunkSize * Long.BYTES :
                                        seekableByteChannel.size() - numberOfChunk * chunkSize * Long.BYTES;
                                log.info("numberOfChunk "+numberOfChunk);
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
                    }
                    catch(Exception e) {
                        throw e;
                    }
                    finally {
                        boolean isUnlocked = lock.compareAndSet(1,0);
                        if(!isUnlocked) {
                            throw new IllegalStateException("Can't unlock locked file");
                        }
                    }
                    return Unit.unit();
                }
        ).f();
    }
}
