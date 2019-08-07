package org.ray.runtime;

import org.ray.api.id.ActorId;
import org.ray.api.id.JobId;
import org.ray.api.id.UniqueId;
import org.ray.runtime.generated.Common.TaskSpec;

public interface WorkerContext {

  /**
   * ID of the current worker.
   */
  UniqueId getCurrentWorkerId();

  /**
   * The ID of the current job.
   */
  JobId getCurrentJobId();

  /**
   * The ID of the current job.
   * @return
   */
  ActorId getCurrentActorId();

  /**
   * The class loader which is associated with the current job.
   */
  ClassLoader getCurrentClassLoader();

  /**
   * Set the current class loader.
   */
  void setCurrentClassLoader(ClassLoader currentClassLoader);

  /**
   * Get the current task.
   */
  TaskSpec getCurrentTask();
}
