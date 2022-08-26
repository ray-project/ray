package io.ray.runtime.context;

import io.ray.api.id.ActorId;
import io.ray.api.id.JobId;
import io.ray.api.id.TaskId;
import io.ray.api.id.UniqueId;
import io.ray.api.runtimeenv.RuntimeEnv;
import io.ray.runtime.generated.Common.Address;
import io.ray.runtime.generated.Common.TaskType;

/** The context of worker. */
public interface WorkerContext {

  /** ID of the current worker. */
  UniqueId getCurrentWorkerId();

  /** ID of the current job. */
  JobId getCurrentJobId();

  /** ID of the current actor. */
  ActorId getCurrentActorId();

  /** Type of the current task. */
  TaskType getCurrentTaskType();

  /** ID of the current task. */
  TaskId getCurrentTaskId();

  Address getRpcAddress();

  /** RuntimeEnv of the current worker or job(for driver). */
  RuntimeEnv getCurrentRuntimeEnv();
}
