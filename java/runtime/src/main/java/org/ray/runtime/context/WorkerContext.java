package org.ray.runtime.context;

import org.ray.api.id.ActorId;
import org.ray.api.id.JobId;
import org.ray.api.id.TaskId;
import org.ray.api.id.UniqueId;
import org.ray.runtime.generated.Common.TaskType;

/**
 * The context of worker.
 */
public interface WorkerContext {

  /**
   * ID of the current worker.
   */
  UniqueId getCurrentWorkerId();

  /**
   * ID of the current job.
   */
  JobId getCurrentJobId();

  /**
   * ID of the current actor.
   */
  ActorId getCurrentActorId();

  /**
   * The class loader that is associated with the current job. It's used for locating classes when
   * dealing with serialization and deserialization in {@link org.ray.runtime.util.Serializer}.
   */
  ClassLoader getCurrentClassLoader();

  /**
   * Set the current class loader.
   */
  void setCurrentClassLoader(ClassLoader currentClassLoader);

  /**
   * Type of the current task.
   */
  TaskType getCurrentTaskType();

  /**
   * ID of the current task.
   */
  TaskId getCurrentTaskId();
}
