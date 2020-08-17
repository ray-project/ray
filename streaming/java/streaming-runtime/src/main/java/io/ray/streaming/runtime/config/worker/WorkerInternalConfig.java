package io.ray.streaming.runtime.config.worker;

import io.ray.streaming.runtime.config.Config;
import org.aeonbits.owner.Mutable;

/**
 * This worker config is used by JobMaster to define the internal configuration of JobWorker.
 */
public interface WorkerInternalConfig extends Config, Mutable {

  String WORKER_NAME_INTERNAL = io.ray.streaming.util.Config.STREAMING_WORKER_NAME;
  String OP_NAME_INTERNAL = io.ray.streaming.util.Config.STREAMING_OP_NAME;

  /**
   * The name of the worker inside the system.
   */
  @DefaultValue(value = "default-worker-name")
  @Key(value = WORKER_NAME_INTERNAL)
  String workerName();

  /**
   * Operator name corresponding to worker.
   */
  @DefaultValue(value = "default-worker-op-name")
  @Key(value = OP_NAME_INTERNAL)
  String workerOperatorName();
}
