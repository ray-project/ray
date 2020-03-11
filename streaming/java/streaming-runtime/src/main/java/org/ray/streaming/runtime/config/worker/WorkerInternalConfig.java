package org.ray.streaming.runtime.config.worker;

import org.ray.streaming.runtime.config.Config;

/**
 * Worker config.
 * note: Worker config is used for internal.
 */
public interface WorkerInternalConfig extends Config {

  @DefaultValue(value = "default-worker-name")
  @Key(value = org.ray.streaming.util.Config.STREAMING_WORKER_NAME)
  String workerName();

  @DefaultValue(value = "default-worker-op-name")
  @Key(value = org.ray.streaming.util.Config.STREAMING_OP_NAME)
  String workerOperatorName();
}
