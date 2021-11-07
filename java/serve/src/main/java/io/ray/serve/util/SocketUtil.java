package io.ray.serve.util;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

public class SocketUtil {

  public static final int PORT_RANGE_MAX = 65535;

  public static int findAvailableTcpPort(int minPort) {
    int portRange = PORT_RANGE_MAX - minPort;
    int candidatePort = minPort;
    int searchCounter = 0;
    while (!isPortAvailable(candidatePort)) {
      candidatePort++;
      if (++searchCounter > portRange) {
        throw new IllegalStateException(
            String.format(
                "Could not find an available tcp port in the range [%d, %d] after %d attempts.",
                minPort, PORT_RANGE_MAX, searchCounter));
      }
    }
    return candidatePort;
  }

  public static boolean isPortAvailable(int port) {
    ServerSocket socket;
    try {
      socket = new ServerSocket();
    } catch (IOException e) {
      throw new IllegalStateException("Unable to create ServerSocket.", e);
    }

    try {
      InetSocketAddress sa = new InetSocketAddress(port);
      socket.bind(sa);
      return true;
    } catch (IOException ex) {
      return false;
    } finally {
      try {
        socket.close();
      } catch (IOException ex) {
        // ignore this exception for now
      }
    }
  }
}
