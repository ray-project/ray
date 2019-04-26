package org.ray.api.runtimecontext;

import java.util.List;
import org.ray.api.id.UniqueId;

/**
 * A class used for getting information of Ray runtime.
 */
public interface RuntimeContext {

  /**
   * Get the current Driver ID.
   *
   * If called in a driver, this returns the driver ID. If called in a worker, this returns the ID
   * of the associated driver.
   */
  UniqueId getCurrentDriverId();

  /**
   * Get the current actor ID.
   *
   * Note, this can only be called in actors.
   */
  UniqueId getCurrentActorId();

  /**
   * Returns true if the current actor was reconstructed, false if it's created for the first time.
   *
   * Note, this method should only be called from an actor creation task.
   */
  boolean wasCurrentActorReconstructed();

  /**
   * Get the raylet socket name.
   */
  String getRayletSocketName();

  /**
   * Get the object store socket name.
   */
  String getObjectStoreSocketName();

  /**
   * Return true if Ray is running in single-process mode, false if Ray is running in cluster mode.
   */
  boolean isSingleProcess();

  /**
   * Get all node information in Ray cluster.
   */
  List<NodeInfo> getAllNodeInfo();
}
