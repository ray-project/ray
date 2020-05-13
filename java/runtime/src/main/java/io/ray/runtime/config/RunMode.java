package io.ray.runtime.config;

public enum RunMode {

  /**
   * Ray is running in one single Java process, without Raylet backend, object store, and GCS.
   * It's useful for debug.
   */
  SINGLE_PROCESS,

  /**
   * Ray is running on one or more nodes, with multiple processes.
   */
  CLUSTER,
}
