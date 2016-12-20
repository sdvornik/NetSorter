package com.yahoo.sdvornik.main;

import com.yahoo.sdvornik.broadcastlistener.BroadcastListener;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main class for worker node
 */
public enum Worker {

    INSTANCE;

    private final Logger log = LoggerFactory.getLogger(Worker.class.getName());

    private Channel masterNodeChannel;

    private BroadcastListener broadcastListener;

    public static void main(String[] args) throws Exception {
        new BroadcastListener().blockingInit();
    }

    /**
     * Setter for BroadcastListener instance
     * @param broadcastListener
     */
    public void setBroadcastListener(BroadcastListener broadcastListener) {
        this.broadcastListener = broadcastListener;
    }

    /**
     * Getter for BroadcastListener instance
     * @return
     */
    public BroadcastListener getBroadcastListener() {
        return broadcastListener;
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
