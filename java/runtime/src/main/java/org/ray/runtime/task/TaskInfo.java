package org.ray.runtime.task;

import org.ray.api.id.TaskId;
import org.ray.api.id.UniqueId;

public class TaskInfo {
  private final TaskId taskId;
  private final UniqueId jobId;
  private final boolean isActorCreationTask;
  private final boolean isActorTask;

  public TaskInfo(TaskId taskId, UniqueId jobId, boolean isActorCreationTask,
                  boolean isActorTask) {
    this.taskId = taskId;
    this.jobId = jobId;
    this.isActorCreationTask = isActorCreationTask;
    this.isActorTask = isActorTask;
  }

  public TaskId getTaskId() {
    return taskId;
  }

  public UniqueId getJobId() {
    return jobId;
  }

  public boolean isActorCreationTask() {
    return isActorCreationTask;
  }

  public boolean isActorTask() {
    return isActorTask;
  }
}
