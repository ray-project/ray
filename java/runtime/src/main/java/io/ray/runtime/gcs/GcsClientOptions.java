package io.ray.runtime.gcs;

import com.google.common.base.Preconditions;
import io.ray.runtime.config.RayConfig;
import java.net.UnknownHostException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Options to create GCS Client. */
public class GcsClientOptions {
  public String ip;
  public int port;
  public String username;
  public String password;

  // writing as a function in case code reuse is needed later
  private static String[] getIpAndPortForAddress(String address) throws UnknownHostException {
    // Similar to python/ray/_private/net.py
    // do not attempt to parse if not ip:port or url-ish (*://ip:port/*) with
    // ip:port for DNS, IPv4, and IPv6 supported
    Pattern pattern = Pattern.compile("^(.*/)?\\[?(.*[^:\\]])\\]?:([0-9]+)(/.*)?$");
    Matcher matcher = pattern.matcher(address);
    if(!matcher.matches()) {
      throw new UnknownHostException("Must be ip:port or url-ish (*://ip:port/*)");
    }
    String ip = matcher.group(2);
    String port = matcher.group(3);
    String[] ipAndPort = new String[]{ip, port};
    return ipAndPort;
  }

  public GcsClientOptions(RayConfig rayConfig) {
    String[] ipAndPort;
    try {
      ipAndPort = getIpAndPortForAddress(rayConfig.getBootstrapAddress());
    } catch(UnknownHostException ignored) {
      // fall back to old behavior
      ipAndPort = rayConfig.getBootstrapAddress().split(":");
    }
    Preconditions.checkArgument(ipAndPort.length == 2, "Invalid bootstrap address.");
    ip = ipAndPort[0];
    port = Integer.parseInt(ipAndPort[1]);
    username = rayConfig.redisUsername;
    password = rayConfig.redisPassword;
  }
}
