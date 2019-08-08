package org.ray.runtime.gcs;

import org.ray.runtime.config.RayConfig;

public class GcsClientOptions {
  public String ip;
  public int port;
  public String password;
  public boolean isTestClient;

  public GcsClientOptions(RayConfig rayConfig) {
    ip = rayConfig.getRedisIp();
    port = rayConfig.getRedisPort();
    password = rayConfig.redisPassword;
  }
}
