package org.ray.runtime.nativeTypes;

import org.ray.runtime.config.RayConfig;

public class NativeGcsClientOptions {
  public String ip;
  public int port;
  public String password;
  public boolean isTestClient;

  public NativeGcsClientOptions(RayConfig rayConfig) {
    ip = rayConfig.getRedisIp();
    port = rayConfig.getRedisPort();
    password = rayConfig.redisPassword;
  }
}
