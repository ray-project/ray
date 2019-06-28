package org.ray.runtime.task;

import org.ray.api.id.TaskId;
import org.ray.api.id.UniqueId;

public class TaskInfo {
  private final TaskId taskId;
  private final UniqueId jobId;
  private final TaskType taskType;

  public TaskInfo(TaskId taskId, UniqueId jobId, TaskType taskType) {
    this.taskId = taskId;
    this.jobId = jobId;
    this.taskType = taskType;
  }

  public TaskId getTaskId() {
    return taskId;
  }

  public UniqueId getJobId() {
    return jobId;
  }

  public TaskType getTaskType() {
    return taskType;
  }
}
