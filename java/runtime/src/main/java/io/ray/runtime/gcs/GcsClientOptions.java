package io.ray.runtime.gcs;

import io.ray.runtime.config.RayConfig;

/**
 * Options to create GCS Client.
 */
public class GcsClientOptions {
  public String ip;
  public int port;
  public String password;

  public GcsClientOptions(RayConfig rayConfig) {
    ip = rayConfig.getRedisIp();
    port = rayConfig.getRedisPort();
    password = rayConfig.redisPassword;
  }
}
