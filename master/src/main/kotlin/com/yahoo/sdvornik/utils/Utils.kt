package com.yahoo.sdvornik.utils

import fj.Try
import java.net.InetAddress
import java.net.NetworkInterface
import java.net.SocketException
import java.net.UnknownHostException
import java.util.*

/**
 * @author Serg Dvornik <sdvornik@yahoo.com>
 */
object Utils {

    @Throws(UnknownHostException::class, SocketException::class)
    fun getLocalHostAddress(): InetAddress =
            EnumerationToIterable(NetworkInterface.getNetworkInterfaces())
                    .map { iface -> EnumerationToIterable(iface.inetAddresses).toList() }.toList().flatten()
                    .filter { address  -> !address.isLoopbackAddress && address.isSiteLocalAddress }
                    .getOrElse(0, { InetAddress.getLocalHost() } )

    private class EnumerationToIterable<T>(private val enumeration: Enumeration<T>) : Iterable<T> {

        override fun iterator(): Iterator<T> = object : Iterator<T> {

            override fun hasNext(): Boolean = enumeration.hasMoreElements()

            override fun next(): T =  enumeration.nextElement()
        }
    }
}
