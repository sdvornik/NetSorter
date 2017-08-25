package com.yahoo.sdvornik.handlers

import com.yahoo.sdvornik.clients.BroadcastListener
import com.yahoo.sdvornik.main.WorkerEntryPoint
import com.yahoo.sdvornik.message.DataMessageArray
import com.yahoo.sdvornik.message.Message
import com.yahoo.sdvornik.message.StartSortingMessage
import com.yahoo.sdvornik.sorter.QuickSort
import com.yahoo.sdvornik.sorter.merger.Merger
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import org.slf4j.LoggerFactory
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.ReentrantLock

/**
 * @author Serg Dvornik <sdvornik@yahoo.com>
 */
class WorkerClientHandler(private val connectionLock: ReentrantLock, private val condition: Condition) : ChannelInboundHandlerAdapter() {

    private var taskCounter = 0

    override fun channelActive(ctx: ChannelHandlerContext) {

        ctx.channel().closeFuture().addListener {
            WorkerEntryPoint.INSTANCE.masterNodeChannel = null
            log.info("WorkerEntryPoint node lost connection with master node. Start broadcast listener.")
            BroadcastListener().blockingInit()
        }
    }

    @Throws(Exception::class)
    override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
        if (msg is Message) {
            val type = (msg as Message).type
            when (type) {
                Message.Type.CONNECTED -> {
                    log.info("Worker node successfully join to cluster.")
                    try {
                        connectionLock.lock()
                        condition.signalAll()
                    } finally {
                        connectionLock.unlock()
                    }
                    WorkerEntryPoint.INSTANCE.masterNodeChannel = ctx.channel()
                }
                Message.Type.TASK_TRANSMISSION_ENDED -> log.info("Receive stop task transmission message")
                Message.Type.GET_RESULT -> {
                    log.info("Receive get result message")
                    Merger.INSTANCE.sendResult()
                }
                Message.Type.START_SORTING -> {
                    val taskCounter = (msg as StartSortingMessage).content
                    Merger.INSTANCE.init(taskCounter)
                    log.info("Worker node prepared for sorting operation. Number of chunks " + taskCounter)
                }
            }//TODO implement interrupt Merger thread on timeout
        } else if (msg is DataMessageArray) {
            ++taskCounter

            ctx.executor().execute(
                    object : Runnable {
                        override fun run() {
                            var presortedArr = (msg as DataMessageArray).array
                            presortedArr = QuickSort(presortedArr).sort()
                            Merger.INSTANCE.putArrayInQueue(presortedArr)
                        }
                    }
            )

        }
    }

    override fun exceptionCaught(ctx: ChannelHandlerContext, e: Throwable) {
        log.error("Exception in WorkerHandler", e)
        ctx.close()
    }

    companion object {

        private val log = LoggerFactory.getLogger(WorkerClientHandler::class.java!!)
    }
}
