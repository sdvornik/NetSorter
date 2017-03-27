package com.yahoo.sdvornik.message.codec;

import com.yahoo.sdvornik.message.DataMessageBuffer;
import com.yahoo.sdvornik.message.Message;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MsgToByteEncoder extends MessageToByteEncoder<Message>{

    private static final Logger log = LoggerFactory.getLogger(MsgToByteEncoder.class);

    @Override
    protected void encode(ChannelHandlerContext ctx, Message msg, ByteBuf out) throws Exception {
        msg.writeHeader(out);
        switch(msg.getType()) {
            case DATA_TYPE :
            case START_SORTING:
                msg.writeContent(out);
        }
    }
}
