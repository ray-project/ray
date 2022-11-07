package io.ray.api.runtimecontext;

import io.ray.api.BaseActorHandle;
import io.ray.api.id.ActorId;
import io.ray.api.id.JobId;
import io.ray.api.id.TaskId;
import io.ray.api.id.UniqueId;
import io.ray.api.runtimeenv.RuntimeEnv;
import java.util.List;

/** A class used for getting information of Ray runtime. */
public interface RuntimeContext {

  /** Get the current Job ID. */
  JobId getCurrentJobId();

  /** Get current task ID. */
  TaskId getCurrentTaskId();

  /**
   * Get the current actor ID.
   *
   * <p>Note, this can only be called in actors.
   */
  ActorId getCurrentActorId();

  /** Returns true if the current actor was restarted, otherwise false. */
  boolean wasCurrentActorRestarted();

  /** Returns true if Ray is running in local mode, false if Ray is running in cluster mode. */
  boolean isLocalMode();

  /** Get all node information in Ray cluster. */
  List<NodeInfo> getAllNodeInfo();

  /**
   * Get the handle to the current actor itself. Note that this method must be invoked in an actor.
   */
  <T extends BaseActorHandle> T getCurrentActorHandle();

  /** Get available GPU(deviceIds) for this worker. */
  List<Long> getGpuIds();

  /** Get the namespace of this job. */
  String getNamespace();

  /** Get the node id of this worker. */
  UniqueId getCurrentNodeId();

  /**
   * Get the runtime env of this worker. If it is a driver, job level runtime env will be returned.
   */
  RuntimeEnv getCurrentRuntimeEnv();
}
