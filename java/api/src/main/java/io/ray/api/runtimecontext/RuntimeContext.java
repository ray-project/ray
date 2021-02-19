package io.ray.api.runtimecontext;

import io.ray.api.id.ActorId;
import io.ray.api.id.JobId;
import java.util.List;

/** A class used for getting information of Ray runtime. */
public interface RuntimeContext {

  /** Get the current Job ID. */
  JobId getCurrentJobId();

  /**
   * Get the current actor ID.
   *
   * <p>Note, this can only be called in actors.
   */
  ActorId getCurrentActorId();

  /** Returns true if the current actor was restarted, otherwise false. */
  boolean wasCurrentActorRestarted();

  /**
   * Returns true if Ray is running in single-process mode, false if Ray is running in cluster mode.
   */
  boolean isSingleProcess();

  /** Get all node information in Ray cluster. */
  List<NodeInfo> getAllNodeInfo();
}
