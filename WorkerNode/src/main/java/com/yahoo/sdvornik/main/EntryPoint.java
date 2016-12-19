package com.yahoo.sdvornik.main;

import com.yahoo.sdvornik.broadcastlistener.BroadcastListener;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class EntryPoint {

    private static final Logger log = LoggerFactory.getLogger(EntryPoint.class.getName());

    private static Channel masterNodeChannel;

    public static void main(String[] args) throws Exception {

        BroadcastListener listener = new BroadcastListener();
        listener.init();
    }

    public static void setMasterNodeChannel(Channel channel) {
       masterNodeChannel = channel;
    }

    public static Channel getMasterNodeChannel() {
        return masterNodeChannel;
    }
}
