package io.ray.runtime.config;

public enum RunMode {

  /**
   * No longer supported.
   */
  LOCAL,

  /** Ray is running on one or more nodes, with multiple processes. */
  CLUSTER,
}
