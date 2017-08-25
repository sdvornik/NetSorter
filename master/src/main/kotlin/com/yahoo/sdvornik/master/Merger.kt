package com.yahoo.sdvornik.master

import fj.Unit
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingDeque

/**
 * @author Serg Dvornik <sdvornik@yahoo.com>
 */
class Merger(
        private val idList: fj.data.List<String>,
        private val numberOfKeys: Long,
        private val totalKeysInOneGeneration: Int,
        private val pathToResult: Path,
        private val before: fj.F0<Unit>?,
        private val onError: fj.F<String, Unit>?,
        private val onSuccess: fj.F0<Unit>?
) {


    private val dequeMap = HashMap<String, LinkedBlockingDeque<LongArray>>()
    private val executor = Executors.newFixedThreadPool(1)

    private val mergeTask = Runnable {
        before?.f()

        val maxGeneration = numberOfKeys.toInt() / totalKeysInOneGeneration + if (numberOfKeys.toInt() % totalKeysInOneGeneration == 0) 0 else 1

        try {
            multiMergeAndSave(idList.array(Array<String>::class.java), numberOfKeys, totalKeysInOneGeneration)
            onSuccess?.f()
        } catch (e: InterruptedException) {
            onError?.f("Unexpected InterruptedException")
        }

        dequeMap.clear()
    }

    fun init() {
        idList.foreach { id ->
            dequeMap.put(id, LinkedBlockingDeque())
            Unit.unit()
        }
        executor.execute(mergeTask)
        log.info("Init Merger")
    }

    fun shutdownNow() {
        dequeMap.clear()
        executor.shutdownNow()
    }

    @Throws(InterruptedException::class)
    fun multiMergeAndSave(id: Array<String>, numberOfKeys: Long, totalKeysInOneGeneration: Int) {

        val maxGeneration = numberOfKeys.toInt() / totalKeysInOneGeneration + if (numberOfKeys.toInt() % totalKeysInOneGeneration == 0) 0 else 1

        val multiArr = arrayOfNulls<LongArray>(id.size)
        val curIndex = IntArray(id.size)
        val curGeneration = IntArray(id.size)

        var curNumberOfArrWithMinValue = -1
        var generation = 0

        for (i in id.indices) {
            multiArr[i] = dequeMap[id[i]]?.takeFirst()
            curGeneration[i] = 1
        }

        try {
            Files.newByteChannel(pathToResult,
                    EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.APPEND)).use { writableByteChannel ->

                val buffer = ByteBuffer.allocate(totalKeysInOneGeneration * java.lang.Long.BYTES)

                while (generation < maxGeneration) {
                    val mergeArrLength: Int = if (generation < maxGeneration - 1) totalKeysInOneGeneration
                    else numberOfKeys.toInt() - totalKeysInOneGeneration * generation
                    val mergeArr = LongArray(mergeArrLength)
                    for (i in 0..mergeArrLength - 1) {
                        var curMinValue = java.lang.Long.MAX_VALUE

                        for (k in multiArr.indices) {
                            val multiArrK: LongArray? = multiArr[k]
                            if (curIndex[k] < multiArrK!!.size && multiArrK[curIndex[k]] < curMinValue) {
                                curMinValue = multiArrK[curIndex[k]]
                                curNumberOfArrWithMinValue = k
                            }
                        }
                        mergeArr[i] = curMinValue
                        ++curIndex[curNumberOfArrWithMinValue]

                        if (curIndex[curNumberOfArrWithMinValue] == multiArr[curNumberOfArrWithMinValue]?.size) {

                            if (curGeneration[curNumberOfArrWithMinValue] < maxGeneration) {

                                val deque = dequeMap[id[curNumberOfArrWithMinValue]]

                                multiArr[curNumberOfArrWithMinValue] = deque?.takeFirst()

                                curIndex[curNumberOfArrWithMinValue] = 0
                                ++curGeneration[curNumberOfArrWithMinValue]
                            }
                        }
                    }

                    for (i in mergeArr.indices) {
                        buffer.putLong(mergeArr[i])
                    }
                    buffer.flip()
                    writableByteChannel.write(buffer)
                    buffer.clear()

                    ++generation
                }
            }
        } catch (e: IOException) {
            log.error("Unexpected error while writing to file.", e)
        }

    }

    fun putArrayInQueue(id: String, numberOfChunk: Int, sortedArr: LongArray) {
        val deque = dequeMap[id]
        deque?.addLast(sortedArr)
    }

    companion object {

        private val log = LoggerFactory.getLogger(MasterTask::class.java)
    }
}
