package com.yahoo.sdvornik.main

import com.yahoo.sdvornik.clients.BroadcastListener
import io.netty.channel.Channel

/**
 * @author Serg Dvornik <sdvornik@yahoo.com>
 */
/**
 * Main class for worker node
 */
enum class WorkerEntryPoint {

    INSTANCE;

    private val broadcastListener = BroadcastListener()

    /**
     * Setter for master node channel
     * @param channel
     */
    var masterNodeChannel: Channel? = null

    /**
     * Stop broadcast listener
     */
    fun stopBroadcastListener() {
        this.broadcastListener.stop()
    }

    companion object {

        @Throws(Exception::class)
        @JvmStatic
        fun main(args: Array<String>) {
            WorkerEntryPoint.INSTANCE.broadcastListener.blockingInit()
        }
    }
}