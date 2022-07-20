package io.ray.runtime.util;

import com.google.common.base.Strings;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetworkUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(NetworkUtil.class);

  public static String getIpAddress(String interfaceName) {
    try {
      Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
      while (interfaces.hasMoreElements()) {
        NetworkInterface current = interfaces.nextElement();
        if (!current.isUp() || current.isLoopback() || current.isVirtual()) {
          continue;
        }
        if (!Strings.isNullOrEmpty(interfaceName)
            && !interfaceName.equals(current.getDisplayName())) {
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
      LOGGER.warn("You need to correctly specify [ray.java] net_interface in config.");
    } catch (Exception e) {
      LOGGER.error("Can't get ip address, use 127.0.0.1 as default.", e);
    }

    return "127.0.0.1";
  }

  public static String localhostIp() {
    return "127.0.0.1";
  }
}
