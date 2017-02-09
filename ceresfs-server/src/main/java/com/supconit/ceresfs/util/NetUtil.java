package com.supconit.ceresfs.util;

import com.google.common.base.Preconditions;

import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.zookeeper.ClientCnxn;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;

public class NetUtil {

    public static InetAddress getLocalHost() throws UnknownHostException {
        return InetAddress.getLocalHost();
    }

    /**
     * Get the local network address for specified remote server
     */
    public static InetAddress getLocalAddressFromRemoteServer(String remoteHost, int remotePort)
            throws IOException {

        try (Socket socket = new Socket(remoteHost, remotePort)) {
            return socket.getLocalAddress();
        }
    }

    public static InetAddress getLocalAddressFromZookeeper(CuratorFramework client) throws Exception {
        Preconditions.checkState(client.getState().equals(CuratorFrameworkState.STARTED));
        CuratorZookeeperClient zookeeperClient = client.getZookeeperClient();
        while (!zookeeperClient.isConnected())
            Thread.yield();

        ZooKeeper zookeeper = zookeeperClient.getZooKeeper();
        Class<?> clazz = zookeeper.getClass();
        Field field = clazz.getDeclaredField("cnxn");
        field.setAccessible(true);
        ClientCnxn cnxn = (ClientCnxn) field.get(zookeeper);
        clazz = cnxn.getClass();
        field = clazz.getDeclaredField("sendThread");
        field.setAccessible(true);
        Object sendThread = field.get(cnxn);
        Method method = sendThread.getClass().getDeclaredMethod("getClientCnxnSocket");
        method.setAccessible(true);
        Object clientCnxnSocket = method.invoke(sendThread);
        method = clientCnxnSocket.getClass().getDeclaredMethod("getLocalSocketAddress");
        method.setAccessible(true);
        InetSocketAddress socketAddress = (InetSocketAddress) method.invoke(clientCnxnSocket);
        return socketAddress.getAddress();
    }
}
