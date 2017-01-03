package com.yahoo.sdvornik.message.codec;

import com.yahoo.sdvornik.message.DataMessageBuffer;
import com.yahoo.sdvornik.message.Message;
import com.yahoo.sdvornik.message.StartSortingMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ByteBufToMsgDecoder extends MessageToMessageDecoder<ByteBuf> {

    private static final Logger log = LoggerFactory.getLogger(ByteBufToMsgDecoder.class.getName());


    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {

        int typeInt = in.readInt();

        Message.Type type = Message.toEnum(typeInt);
        switch(type) {
            case GET_CONNECTION :
            case JOB_ENDED:
            case TASK_TRANSMISSION_ENDED:
            case GET_RESULT:
            case CONNECTED:
                out.add(Message.getSimpleOutboundMessage(type));
                break;

            case DATA_TYPE:
                int chunkNumber = in.readInt();
                out.add(new DataMessageBuffer(in, chunkNumber));
                in.retain();
                break;

            case START_SORTING:
                int taskCounter = in.readInt();
                out.add(new StartSortingMessage(taskCounter));
                break;
        }
    }
}
