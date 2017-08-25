package com.yahoo.sdvornik.handlers

import com.yahoo.sdvornik.main.MasterEntryPoint
import com.yahoo.sdvornik.master.MasterTask
import com.yahoo.sdvornik.message.DataMessageArray
import com.yahoo.sdvornik.message.Message
import io.netty.channel.*
import org.slf4j.LoggerFactory

/**
 * @author Serg Dvornik <sdvornik@yahoo.com>
 */
@ChannelHandler.Sharable
class MasterServerHandler : ChannelInboundHandlerAdapter() {

    @Throws(Exception::class)
    override fun channelActive(ctx: ChannelHandlerContext) {
        val id = ctx.channel().id().asShortText()
        val joinMsg = "Worker node with id: $id try to join into cluster."
        log.info(joinMsg)
        MasterEntryPoint.INSTANCE.sendMsgToWebSocketGroup(joinMsg)
    }

    override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {

        if (msg is Message) {
            val type = msg.type
            when (type) {
                Message.Type.GET_CONNECTION -> {
                    log.info("Receive GET_CONNECTION_MESSAGE. Send CONNECTED_MESSAGE.")
                    val id = ctx.channel().id().asShortText()

                    val future = ctx.writeAndFlush(Message.getSimpleOutboundMessage(Message.Type.CONNECTED))
                    future.addListener {
                        MasterEntryPoint.INSTANCE.addChannelToMasterGroup(ctx.channel())
                        val joinMsg = "Worker node with id: $id joined into cluster."
                        log.info(joinMsg)
                        MasterEntryPoint.INSTANCE.sendMsgToWebSocketGroup(joinMsg)
                        val closeFuture = ctx.channel().closeFuture()

                        closeFuture.addListener {
                            MasterEntryPoint.INSTANCE.removeChannelFromMasterGroup(ctx.channel())
                            val leftMsg = "Node with id: $id left the cluster."
                            log.info(leftMsg)
                            MasterEntryPoint.INSTANCE.sendMsgToWebSocketGroup(leftMsg)
                        }
                    }
                }
                Message.Type.JOB_ENDED -> {
                    log.info("Receive JOB_ENDED_MESSAGE. Save this fact.")
                    MasterTask.INSTANCE.saveResponse(ctx.channel().id().asShortText())
                }
            }
        }
        else if (msg is DataMessageArray) {
            MasterTask.INSTANCE.putArrayInQueue(
                    ctx.channel().id().asShortText(),
                    msg.chunkNumber,
                    msg.array
            )
        }

    }

    override fun exceptionCaught(ctx: ChannelHandlerContext, e: Throwable) {
        log.error("Exception in MasterEntryPoint handler", e)
        ctx.close()
    }

    companion object {

        private val log = LoggerFactory.getLogger(MasterServerHandler::class.java)
    }
}
