package io.ray.runtime.gcs;

import com.google.common.base.Preconditions;
import io.ray.runtime.config.RayConfig;

/** Options to create GCS Client. */
public class GcsClientOptions {
  public String ip;
  public int port;
  public String password;

  public GcsClientOptions(RayConfig rayConfig) {
    String[] ipAndPort = rayConfig.getBootstrapAddress().split(":");
    Preconditions.checkArgument(ipAndPort.length == 2, "Invalid bootstrap address.");
    ip = ipAndPort[0];
    port = Integer.parseInt(ipAndPort[1]);
    password = rayConfig.redisPassword;
  }
}
