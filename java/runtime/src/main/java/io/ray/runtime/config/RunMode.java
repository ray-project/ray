package io.ray.runtime.config;

public enum RunMode {

  /**
   * Ray is running in one single Java process, without Raylet backend, object store, and GCS. It's
   * useful for debug.
   */
  LOCAL,

  /** Ray is running on one or more nodes, with multiple processes. */
  CLUSTER,
}
