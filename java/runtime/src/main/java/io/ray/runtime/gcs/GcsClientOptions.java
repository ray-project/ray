package io.ray.runtime.gcs;

import com.google.common.base.Preconditions;
import io.ray.runtime.config.RayConfig;
import io.ray.runtime.util.SystemConfig;

/** Options to create GCS Client. */
public class GcsClientOptions {
  public String bootstrap_address;
  public String redis_ip;
  public int redis_port;
  public String redis_password;

  public GcsClientOptions(RayConfig rayConfig) {
      bootstrap_address = rayConfig.bootstrapAddress;
      if(SystemConfig.bootstrapWithGcs()) {
          String[] ipAndPort = rayConfig.getRedisAddress().split(":");
          Preconditions.checkArgument(ipAndPort.length == 2, "Invalid redis address.");
          redis_ip = ipAndPort[0];
          redis_port = Integer.parseInt(ipAndPort[1]);
          redis_password = rayConfig.redisPassword;
      } else {
          redis_ip = null;
          redis_port = -1;
          redis_password = null;
      }
  }
}
