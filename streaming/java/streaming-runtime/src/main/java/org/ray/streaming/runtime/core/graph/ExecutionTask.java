package org.ray.streaming.runtime.core.graph;

import java.io.Serializable;
import org.ray.api.BaseActor;

/**
 * ExecutionTask is minimal execution unit.
 * <p>
 * An ExecutionNode has n ExecutionTasks if parallelism is n.
 */
public class ExecutionTask implements Serializable {
  private int taskId;
  private int taskIndex;
  private BaseActor worker;

  public ExecutionTask(int taskId, int taskIndex, BaseActor worker) {
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

  public BaseActor getWorker() {
    return worker;
  }

  public void setWorker(BaseActor worker) {
    this.worker = worker;
  }
}
