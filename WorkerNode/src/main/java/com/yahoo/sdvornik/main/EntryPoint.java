package com.yahoo.sdvornik.main;

import com.yahoo.sdvornik.Constants;
import com.yahoo.sdvornik.broadcastlistener.BroadcastListener;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public class EntryPoint {

    private static final Logger log = LoggerFactory.getLogger(EntryPoint.class.getName());

    public static void main(String[] args) throws Exception {

        BroadcastListener listener = new BroadcastListener();
        listener.init();
    }
}
