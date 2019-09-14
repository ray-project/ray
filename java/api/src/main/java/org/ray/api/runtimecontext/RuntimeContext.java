package org.ray.api.runtimecontext;

import java.util.List;
import org.ray.api.id.ActorId;
import org.ray.api.id.JobId;

/**
 * A class used for getting information of Ray runtime.
 */
public interface RuntimeContext {

  /**
   * Get the current Job ID.
   */
  JobId getCurrentJobId();

  /**
   * Get the current actor ID.
   *
   * Note, this can only be called in actors.
   */
  ActorId getCurrentActorId();

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
