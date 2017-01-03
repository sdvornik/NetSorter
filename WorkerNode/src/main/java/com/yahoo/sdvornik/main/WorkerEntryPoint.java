package com.yahoo.sdvornik.main;

import com.yahoo.sdvornik.clients.BroadcastListener;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main class for worker node
 */
public enum WorkerEntryPoint {

    INSTANCE;

    private final Logger log = LoggerFactory.getLogger(WorkerEntryPoint.class.getName());

    private final BroadcastListener broadcastListener = new BroadcastListener();

    private Channel masterNodeChannel;

    public static void main(String[] args) throws Exception {
        WorkerEntryPoint.INSTANCE.broadcastListener.blockingInit();
    }

    /**
     * Stop broadcast listener
     */
    public void stopBroadcastListener() {
        this.broadcastListener.stop();
    }

    /**
     * Setter for master node channel
     * @param channel
     */
    public void setMasterNodeChannel(Channel channel) {
       masterNodeChannel = channel;
    }

    /**
     * Getter for master node channel
     * @return
     */
    public Channel getMasterNodeChannel() {
        return masterNodeChannel;
    }
}
