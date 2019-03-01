package org.ray.api;

import org.ray.api.id.UniqueId;

/**
 * A class used for getting runtime context fields.
 */
public interface RuntimeContext {

  /**
   * Get the current Driver id. If called by a driver,
   * this returns the driver ID. If called in a task,
   * return the driver ID of the associated driver.
   */
  UniqueId getCurrentDriverId();

  /**
   * Get the current actor id. This only could be
   * called in actor task, not in driver or normal task.
   */
  UniqueId getCurrentActorId();

  /**
   * Whether the current actor was reconstructed.
   */
  boolean wasCurrentActorReconstructed();

  /**
   * Get the raylet socket name that we can connect to.
   */
  String getRayletSocketName();

  /**
   * Get the object store socket name that we can connect to.
   */
  String getObjectStoreSocketName();

  /**
   * Whether the run mode equals `SINGLE_PROCESS`.
   */
  boolean isSingleProcess();
}
