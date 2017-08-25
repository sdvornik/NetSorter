package com.yahoo.sdvornik.utils

import com.yahoo.sdvornik.Constants
import fj.Try
import fj.data.Validation
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
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
/**
 * Test file generator. Has two implementations for
 * key generator - random and sequential.
 */
enum class KeyGenerator {

    INSTANCE;

    private val random = ThreadLocalRandom.current()
    private val buffer = ByteBuffer.allocate(java.lang.Long.BYTES)
    private val lock = AtomicInteger(0)
    private var counter: Long = 0


    private fun generateRandomKey(): ByteArray {
        buffer.putLong(random.nextLong())
        buffer.flip()
        val byteArr = buffer.array()
        buffer.clear()
        return byteArr
    }

    private fun generateSequentialKey(): ByteArray {
        buffer.putLong(++counter)
        buffer.flip()
        val byteArr = buffer.array()
        buffer.clear()
        return byteArr
    }

    /**
     * Generate file
     * @param size_in_mbytes
     * @return
     */
    fun generateFile(size_in_mbytes: Int?): Validation<out Exception, Path> {

        val pathToFile = Paths.get(System.getProperty("user.home"), Constants.DEFAULT_FILE_NAME)
        val size_in_bytes = (if (size_in_mbytes != null && size_in_mbytes < Constants.MAX_FILE_SIZE_IN_MBYTES)
            size_in_mbytes
        else
            Constants.MAX_FILE_SIZE_IN_MBYTES) * Constants.BYTES_IN_MBYTES


        return Try.f<Path, IOException> {
            val isLocked = !lock.compareAndSet(0, 1)
            if (isLocked) {
                throw IllegalStateException("File is locked")
            }
            try {
                Files.deleteIfExists(pathToFile)
                Files.createFile(pathToFile)
                writeToFile(pathToFile, size_in_bytes)
            } catch (e: Exception) {
                throw e
            } finally {
                val isUnlocked = lock.compareAndSet(1, 0)
                if (!isUnlocked) {
                    throw IllegalStateException("Can't unlock locked file")
                }
            }
            pathToFile
        }.f()

    }

    @Throws(IOException::class)
    private fun writeToFile(pathToFile: Path, fileSize: Long) {
        counter = 0
        FileChannel.open(pathToFile, EnumSet.of(StandardOpenOption.WRITE)).use { writeFileChannel ->
            val bytebuffer = ByteBuffer.allocateDirect(Constants.BUFFER_SIZE_IN_BYTES)

            var bytesCount: Long = 0
            while (bytesCount < fileSize) {
                while (bytebuffer.position() < Constants.BUFFER_SIZE_IN_BYTES - java.lang.Long.BYTES) {
                    bytebuffer.put(INSTANCE.generateSequentialKey())
                }
                bytesCount += bytebuffer.position().toLong()
                bytebuffer.flip()
                writeFileChannel.write(bytebuffer)
                bytebuffer.clear()
            }
        }
    }
}
