package com.yahoo.sdvornik.generator;

import com.yahoo.sdvornik.sharable.Constants;
import fj.Try;
import fj.data.Validation;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public enum KeyGenerator {

    INSTANCE;

    private ThreadLocalRandom random = ThreadLocalRandom.current();
    private ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    private AtomicInteger lock = new AtomicInteger(0);
    private long counter;


    private byte[] generateRandomKey() {
        buffer.putLong(random.nextLong());
        buffer.flip();
        byte[] byteArr = buffer.array();
        buffer.clear();
        return byteArr;
    }

    private byte[] generateSequentialKey() {
        buffer.putLong(++counter);
        buffer.flip();
        byte[] byteArr = buffer.array();
        buffer.clear();
        return byteArr;
    }

    public Validation<? extends Exception, Path> generateFile(Integer size_in_mbytes) {

        final Path pathToFile = Paths.get(System.getProperty("user.home"), Constants.DEFAULT_FILE_NAME);
        final int size_in_bytes = ((size_in_mbytes!=null && size_in_mbytes < Constants.MAX_FILE_SIZE_IN_MBYTES) ?
                size_in_mbytes : Constants.MAX_FILE_SIZE_IN_MBYTES)*Constants.BYTES_IN_MBYTES;


        return Try.f(
                () -> {
                    boolean isLocked = !lock.compareAndSet(0,1);
                    if(isLocked) {
                        throw new IllegalStateException("File is locked");
                    }
                    try {
                        Files.deleteIfExists(pathToFile);
                        Files.createFile(pathToFile);
                        writeToFile(pathToFile, size_in_bytes);
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
                    return pathToFile;
                }
        ).f();

    }

    private void writeToFile(Path pathToFile, long fileSize) throws IOException {
        counter = 0;
        try (
                FileChannel writeFileChannel = (FileChannel.open(pathToFile, EnumSet.of(StandardOpenOption.WRITE)))
        ) {
            ByteBuffer bytebuffer = ByteBuffer.allocateDirect(Constants.BUFFER_SIZE_IN_BYTES);

            long bytesCount=0;
            while(bytesCount < fileSize) {
                while (bytebuffer.position() < Constants.BUFFER_SIZE_IN_BYTES - Long.BYTES) {
                    bytebuffer.put(INSTANCE.generateSequentialKey());
                }
                bytesCount += bytebuffer.position();
                bytebuffer.flip();
                writeFileChannel.write(bytebuffer);
                bytebuffer.clear();
            }
        }
    }
}
