package com.ray.streaming.core.graph;

import com.ray.streaming.core.runtime.StreamWorker;
import java.io.Serializable;
import org.ray.api.RayActor;

/**
 * ExecutionTask is minimal execution unit.
 *
 * A ExecutionNode has n ExecutionTasks if parallelism is n.
 */
public class ExecutionTask implements Serializable {

  private int taskId;
  private int taskIndex;
  private RayActor<StreamWorker> worker;

  public ExecutionTask(int taskId, int taskIndex, RayActor<StreamWorker> worker) {
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

  public RayActor<StreamWorker> getWorker() {
    return worker;
  }

  public void setWorker(RayActor<StreamWorker> worker) {
    this.worker = worker;
  }
}
