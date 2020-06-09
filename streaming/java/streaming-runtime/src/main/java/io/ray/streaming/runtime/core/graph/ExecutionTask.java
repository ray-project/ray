package io.ray.streaming.runtime.core.graph;

import io.ray.api.BaseActorHandle;
import java.io.Serializable;

/**
 * ExecutionTask is minimal execution unit.
 * <p>
 * An ExecutionNode has n ExecutionTasks if parallelism is n.
 */
public class ExecutionTask implements Serializable {
  private int taskId;
  private int taskIndex;
  private BaseActorHandle worker;

  public ExecutionTask(int taskId, int taskIndex, BaseActorHandle worker) {
    this.taskId = taskId;
    this.taskIndex = taskIndex;
    this.worker = worker;
  }

  public int getTaskId() {
    return taskId;
  }

  public void setTaskId(int taskId) {
    this.taskId = taskId;
  }

  public int getTaskIndex() {
    return taskIndex;
  }

  public void setTaskIndex(int taskIndex) {
    this.taskIndex = taskIndex;
  }

  public BaseActorHandle getWorker() {
    return worker;
  }

  public void setWorker(BaseActorHandle worker) {
    this.worker = worker;
  }
}
