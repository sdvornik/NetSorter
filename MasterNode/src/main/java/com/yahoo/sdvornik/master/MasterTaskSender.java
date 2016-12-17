package com.yahoo.sdvornik.master;

import io.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;

public class MasterTaskSender {

    private static final Logger log = LoggerFactory.getLogger(MasterTaskSender.class.getName());

    private Path pathToFile;
    public MasterTaskSender(Path pathToFile) {
        this.pathToFile = pathToFile;
    }

    public void read(int packetNumber, long position, long count, Channel outputChannel) throws Exception {
        RandomAccessFile file = new RandomAccessFile(pathToFile.toFile(), "r");

        if(position+count > file.length()) {
            throw new IllegalArgumentException("Arguments out of file length");
        }
        FileRegion region = new DefaultFileRegion(
                file.getChannel(),
                position,
                count
        );

        outputChannel.writeAndFlush(region).addListener(
            new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (!future.isSuccess()) {
                        log.error("Can't send packet "+packetNumber,future.cause() );
                    }
                    else {
                        //TODO
                    }
                }
            }
        );

    }

}
