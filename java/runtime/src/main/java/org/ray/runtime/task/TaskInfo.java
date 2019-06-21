package org.ray.runtime.task;

import org.ray.api.id.TaskId;
import org.ray.api.id.UniqueId;

public class TaskInfo {
  private final TaskId taskId;
  private final UniqueId driverId;
  private final boolean isActorCreationTask;
  private final boolean isActorTask;

  public TaskInfo(TaskId taskId, UniqueId driverId, boolean isActorCreationTask,
                  boolean isActorTask) {
    this.taskId = taskId;
    this.driverId = driverId;
    this.isActorCreationTask = isActorCreationTask;
    this.isActorTask = isActorTask;
  }

  public TaskId getTaskId() {
    return taskId;
  }

  public UniqueId getDriverId() {
    return driverId;
  }

  public boolean isActorCreationTask() {
    return isActorCreationTask;
  }

  public boolean isActorTask() {
    return isActorTask;
  }
}
