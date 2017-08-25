package com.yahoo.sdvornik.master

import com.yahoo.sdvornik.Constants
import com.yahoo.sdvornik.main.MasterEntryPoint
import com.yahoo.sdvornik.message.DataMessageBuffer
import com.yahoo.sdvornik.message.Message
import com.yahoo.sdvornik.message.StartSortingMessage
import fj.F
import fj.F0
import fj.Unit
import io.netty.channel.Channel
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame
import org.slf4j.LoggerFactory
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import java.util.*
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicInteger

/**
 * @author Serg Dvornik <sdvornik@yahoo.com>
 */
enum class MasterTask {

    INSTANCE;

    private val log = LoggerFactory.getLogger(MasterTask::class.java)

    private val lock = AtomicInteger(0)
    private var pathToFile: Path? = null
    private var wsClientChannel: Channel? = null

    private var channelList: fj.data.List<Channel> = fj.data.List.nil()
    private var responseList: fj.data.List<Boolean> = fj.data.List.nil()
    private var mergerInstance: Merger? = null

    private var numberOfKeys: Long = 0

    internal var before: fj.F0<Unit> = F0 {
        val message = "Start to merge worker nodes results."
        wsClientChannel!!.writeAndFlush(TextWebSocketFrame(message))
        log.info(message)
        Unit.unit()
    }

    internal var onSuccess: fj.F0<Unit> = F0 {
        channelList = fj.data.List.nil()
        responseList = fj.data.List.nil()
        mergerInstance = null

        val isUnlocked = lock.compareAndSet(1, 0)
        if (!isUnlocked) {
            throw RuntimeException()
        }
        val message = "Successfully merged worker nodes results."
        wsClientChannel!!.writeAndFlush(TextWebSocketFrame(message))
        log.info(message)
        Unit.unit()
    }

    internal var onError: fj.F<String, Unit> = F { message ->
        channelList = fj.data.List.nil()
        responseList = fj.data.List.nil()
        mergerInstance = null

        val isUnlocked = lock.compareAndSet(1, 0)
        if (!isUnlocked) {
            throw RuntimeException()
        }
        wsClientChannel!!.writeAndFlush(TextWebSocketFrame(message))
        log.info(message)
        Unit.unit()
    }

    @Throws(Exception::class)
    fun runTask(pathToFile: Path, wsClientChannel: Channel) {
        this.pathToFile = pathToFile
        this.wsClientChannel = wsClientChannel
        this.channelList = fj.data.List.iterableList(MasterEntryPoint.INSTANCE.masterChannelGroup)
        if (channelList.isEmpty) {
            throw IllegalStateException("Nothing worker node are connected to master node")
        }
        val isLocked = !lock.compareAndSet(0, 1)
        if (isLocked) {
            throw IllegalStateException("Operation is locked")
        }
        try {
            distributeTask()
            wsClientChannel.writeAndFlush(TextWebSocketFrame("Succesfully send task to worker node"))
        } catch (e: Exception) {
            onError.f(e.message)
            throw e
        }

    }

    @Throws(Exception::class)
    fun distributeTask() {
        Files.newByteChannel(pathToFile, EnumSet.of(StandardOpenOption.READ)).use { seekableByteChannel ->

            val countOfWorkerNodes = channelList.length()

            numberOfKeys = seekableByteChannel.size() / java.lang.Long.BYTES
            log.info("Total number of keys: " + numberOfKeys)

            val totalChunkQuantity = calcChunkQuantity(numberOfKeys, countOfWorkerNodes)
            val chunkQuantityToOneNode = totalChunkQuantity / countOfWorkerNodes
            log.info("Total number of chunks for one node " + chunkQuantityToOneNode)

            val chunkSize = Math.ceil(numberOfKeys / totalChunkQuantity.toDouble()).toInt()
            log.info("ChunkSize: " + chunkSize)

            val msg = StartSortingMessage(chunkQuantityToOneNode)
            for (outputChannel in channelList) {
                outputChannel.writeAndFlush(msg)
            }

            for (numberOfCycle in 0..chunkQuantityToOneNode - 1) {
                var numberOfNode = 0
                for (outputChannel in channelList) {
                    val buffer = ByteBuffer.allocate(chunkSize * java.lang.Long.BYTES)

                    val numberOfChunk = numberOfCycle * countOfWorkerNodes + numberOfNode

                    seekableByteChannel.position((numberOfChunk * chunkSize * java.lang.Long.BYTES).toLong())
                    seekableByteChannel.read(buffer)
                    buffer.flip()
                    val dataMsg = DataMessageBuffer(buffer, numberOfChunk)

                    outputChannel.writeAndFlush(dataMsg)
                    ++numberOfNode
                }
            }

            for (channel in channelList) {
                channel.writeAndFlush(
                        Message.getSimpleOutboundMessage(Message.Type.TASK_TRANSMISSION_ENDED)
                )
            }
            log.info("Successfully send TASK_TRANSMISSION_ENDED message")
        }
        //TODO set timeout for cancel operation
    }

    fun saveResponse(id: String) {
        wsClientChannel!!.writeAndFlush(TextWebSocketFrame("Worker with id $id finished job"))
        responseList = fj.data.List.cons(java.lang.Boolean.TRUE, responseList)
        if (responseList.length() == channelList.length()) {
            val message = "Start to merge worker nodes results."
            wsClientChannel!!.writeAndFlush(TextWebSocketFrame(message))
            log.info(message)
            responseList = fj.data.List.nil()
            collectResult()
        }

        //TODO set timeout for cancel operation
    }

    private fun collectResult() {

        val idList = channelList.map { channel -> channel.id().asShortText() }
        val pathToResultFile = Paths.get(System.getProperty("user.home"), Constants.OUTPUT_FILE_NAME)
        try {
            Files.deleteIfExists(pathToResultFile)
            Files.createFile(pathToResultFile)
        } catch (e: Exception) {
            log.error("Can't create output file.")
            throw RuntimeException(e)
        }

        mergerInstance = Merger(
                idList,
                numberOfKeys,
                Constants.RESULT_CHUNK_SIZE_IN_KEYS * idList.length(),
                pathToResultFile,
                before,
                onError,
                onSuccess
        )
        mergerInstance!!.init()
        try {
            for (channel in channelList) {
                channel.writeAndFlush(Message.getSimpleOutboundMessage(Message.Type.GET_RESULT)).sync()
            }
            val message = "Successfully send collect message"
            wsClientChannel!!.writeAndFlush(TextWebSocketFrame(message))
            log.info(message)
        } catch (e: Exception) {
            mergerInstance!!.shutdownNow()
            onError.f(e.message)
        }

    }

    fun putArrayInQueue(id: String, numberOfChunk: Int, sortedArr: LongArray) {
        mergerInstance!!.putArrayInQueue(id, numberOfChunk, sortedArr)
    }

    private fun calcChunkQuantity(numberOfKeys: Long, countOfWorkerNodes: Int): Int {
        val parameter = (countOfWorkerNodes * Constants.DEFAULT_CHUNK_SIZE_IN_KEYS).toDouble()
        val power = Math.floor(Math.log(numberOfKeys / parameter) / Math.log(2.0)).toInt()

        return countOfWorkerNodes * Math.pow(2.0, power.toDouble()).toInt()
    }

    companion object {
        private val random = ThreadLocalRandom.current()
    }
}
