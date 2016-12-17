package com.yahoo.sdvornik;

import fj.F0;
import fj.Try;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.Iterator;

public class Utils {

    private Utils() {}

    public static InetAddress getLocalHostAddress() throws UnknownHostException, SocketException {
        return fj.data.List.iterableList(
                new EnumerationToIterable<NetworkInterface>(NetworkInterface.getNetworkInterfaces())
        ).map(
                iface -> fj.data.List.iterableList(
                        new EnumerationToIterable<InetAddress>(iface.getInetAddresses())
                )
        ).foldLeft(
                (fj.data.List<InetAddress> acc, fj.data.List<InetAddress> list) -> acc.append(list), fj.data.List.nil()
        ).filter(
                (InetAddress address) -> !address.isLoopbackAddress() && address.isSiteLocalAddress()
        ).orHead(() -> {
            return Try.f(() -> InetAddress.getLocalHost()).f().toOption().toNull();
        });

    }

    private static class EnumerationToIterable<T> implements Iterable<T> {

        private Enumeration<T> enumeration;

        EnumerationToIterable(Enumeration<T> enumeration) {
            this.enumeration = enumeration;
        }

        @Override
        public Iterator<T> iterator() {
            return new Iterator<T>() {
                public boolean hasNext() {
                    return enumeration.hasMoreElements();
                }
                public T next() {
                    return enumeration.nextElement();
                }
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }
}
