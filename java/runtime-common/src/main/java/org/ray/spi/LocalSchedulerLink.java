package org.ray.spi;

import org.ray.api.UniqueID;
import org.ray.spi.model.TaskSpec;

/**
 * Provides core functionalities of local scheduler.
 */
public interface LocalSchedulerLink {

  void submitTask(TaskSpec task);

  TaskSpec getTaskTodo();

  void markTaskPutDependency(UniqueID taskId, UniqueID objectId);

  void reconstructObject(UniqueID objectId);

  void notifyUnblocked();
}
