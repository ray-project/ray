package org.ray.runtime.util;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.util.Enumeration;
import org.ray.runtime.util.logger.RayLog;

public class NetworkUtil {

  public static String getIpAddress(String interfaceName) {
    try {
      Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
      while (interfaces.hasMoreElements()) {
        NetworkInterface current = interfaces.nextElement();
        if (!current.isUp() || current.isLoopback() || current.isVirtual()) {
          continue;
        }
        if (!StringUtil.isNullOrEmpty(interfaceName) && !interfaceName
            .equals(current.getDisplayName())) {
          continue;
        }
        Enumeration<InetAddress> addresses = current.getInetAddresses();
        while (addresses.hasMoreElements()) {
          InetAddress addr = addresses.nextElement();
          if (addr.isLoopbackAddress()) {
            continue;
          }
          if (addr instanceof Inet6Address) {
            continue;
          }
          return addr.getHostAddress();
        }
      }
      RayLog.core.warn("You need to correctly specify [ray.java] net_interface in config.");
    } catch (Exception e) {
      RayLog.core.error("Can't get ip address, use 127.0.0.1 as default.", e);
    }

    return "127.0.0.1";
  }

  public static boolean isPortAvailable(int port) {
    if (port < 1 || port > 65535) {
      throw new IllegalArgumentException("Invalid start port: " + port);
    }

    try (ServerSocket ss = new ServerSocket(port); DatagramSocket ds = new DatagramSocket(port)) {
      ss.setReuseAddress(true);
      ds.setReuseAddress(true);
      return true;
    } catch (IOException ignored) {
      /* should not be thrown */
      return false;
    }
  }
}
