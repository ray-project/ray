package org.ray.streaming.runtime.config.worker;

import org.ray.streaming.runtime.config.Config;

/**
 * This worker config is used by JobMaster to define the internal configuration of JobWorker.
 */
public interface WorkerInternalConfig extends Config {

  /**
   * The name of the worker inside the system.
   */
  @DefaultValue(value = "default-worker-name")
  @Key(value = org.ray.streaming.util.Config.STREAMING_WORKER_NAME)
  String workerName();

  /**
   * Operator name corresponding to worker.
   */
  @DefaultValue(value = "default-worker-op-name")
  @Key(value = org.ray.streaming.util.Config.STREAMING_OP_NAME)
  String workerOperatorName();
}
