package com.yahoo.sdvornik.sorter.merger

import com.yahoo.sdvornik.Constants
import com.yahoo.sdvornik.main.WorkerEntryPoint
import com.yahoo.sdvornik.message.DataMessageArray
import com.yahoo.sdvornik.message.Message
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue

/**
 * @author Serg Dvornik <sdvornik@yahoo.com>
 */
enum class Merger {

    INSTANCE;

    private val log = LoggerFactory.getLogger(Merger::class.java)

    private val executor = Executors.newFixedThreadPool(1)

    private val queue = LinkedBlockingQueue<LongArray>()

    private val storage = HashMap<Int, LongArray>()

    private var maxTaskNumber: Int = 0

    private var result: LongArray? = null

    private val mergeTask = Runnable {
        var currentTaskNumber = 0
        while (currentTaskNumber < maxTaskNumber) {
            try {
                var arr = queue.take()
                ++currentTaskNumber

                var stage = 0
                while (true) {
                    val savedArr = storage[stage]
                    if (savedArr == null) {
                        storage.put(stage, arr)
                        break
                    } else {
                        // TODO
                        storage.put(stage, LongArray(0))
                        arr = merge(arr, savedArr)
                        ++stage
                    }
                }
            }
            catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
            }
        }
        result = storage.values.stream().filter { arr -> arr != null }.findFirst().get()
        storage.clear()
        queue.clear()
        maxTaskNumber = 0
        log.info("Merging successfully ended. Resulting array length: " + result!!.size + "; first: " + result!![0] + "; last: " + result!![result!!.size - 1])

        val masterChannel = WorkerEntryPoint.INSTANCE.masterNodeChannel
        if (masterChannel != null) {
            masterChannel.writeAndFlush(
                    Message.getSimpleOutboundMessage(Message.Type.JOB_ENDED)
            )
        } else {
            result = null
        }
    }

    /**
     * Unblocking add array in [LinkedBlockingQueue]
     * for further processing.
     * @param arr
     * @return
     */
    fun putArrayInQueue(arr: LongArray): Boolean {
        return queue.add(arr)
    }

    fun init(maxTaskNumber: Int) {
        this.maxTaskNumber = maxTaskNumber
        executor.execute(mergeTask)
    }

    /**
     * Send sorted array to master node.
     * Size of chunk is determined by `Constants.RESULT_CHUNK_SIZE_IN_KEYS`
     * @throws Exception
     */
    @Throws(Exception::class)
    fun sendResult() {
        val totalNumberOfChunk = result!!.size / Constants.RESULT_CHUNK_SIZE_IN_KEYS + if (result!!.size % Constants.RESULT_CHUNK_SIZE_IN_KEYS == 0) 0 else 1

        for (numberOfChunk in 0..totalNumberOfChunk - 1) {

            val count = if (numberOfChunk < totalNumberOfChunk - 1)
                Constants.RESULT_CHUNK_SIZE_IN_KEYS
            else
                result!!.size - numberOfChunk * Constants.RESULT_CHUNK_SIZE_IN_KEYS


            val msg = DataMessageArray(
                    Arrays.copyOfRange(
                            result!!,
                            numberOfChunk * Constants.RESULT_CHUNK_SIZE_IN_KEYS,
                            numberOfChunk * Constants.RESULT_CHUNK_SIZE_IN_KEYS + count
                    ),
                    numberOfChunk
            )

            WorkerEntryPoint.INSTANCE.masterNodeChannel!!.writeAndFlush(msg)

        }
        log.info("Successfully send result")
        result = null
    }

    fun stop() {
        executor.shutdownNow()
    }

    private fun merge(firstArr: LongArray, secondArr: LongArray?): LongArray {
        val resArr = LongArray(firstArr.size + secondArr!!.size)
        var firstIndex = 0
        var secondIndex = 0

        for (i in resArr.indices) {
            if (firstIndex < firstArr.size && secondIndex < secondArr.size) {
                resArr[i] = if (firstArr[firstIndex] < secondArr[secondIndex])
                    firstArr[firstIndex++]
                else
                    secondArr[secondIndex++]
            } else if (firstIndex < firstArr.size) {
                resArr[i] = firstArr[firstIndex++]
            } else {
                resArr[i] = secondArr[secondIndex++]
            }
        }
        return resArr
    }
}
