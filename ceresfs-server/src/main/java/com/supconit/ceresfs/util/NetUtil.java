package com.supconit.ceresfs.util;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

public class NetUtil {

    public static InetAddress getLocalHost() throws UnknownHostException {
        return InetAddress.getLocalHost();
    }

    /**
     * Get the local network address for specified remote server
     */
    public static InetAddress getLocalHostFromRemoteServer(String remoteHost, int remotePort)
            throws IOException {
        try (Socket socket = new Socket(remoteHost, remotePort)) {
            return socket.getLocalAddress();
        }
    }
}
