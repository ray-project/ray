package com.ray.streaming.core.runtime.context;

import com.ray.streaming.core.graph.ExecutionGraph;
import java.io.Serializable;

/**
 * Encapsulate the context information of a worker init.
 */
public class WorkerContext implements Serializable {

  private int taskId;
  private ExecutionGraph executionGraph;

  public WorkerContext(int taskId, ExecutionGraph executionGraph) {
    this.taskId = taskId;
    this.executionGraph = executionGraph;
  }

  public int getTaskId() {
    return taskId;
  }

  public void setTaskId(int taskId) {
    this.taskId = taskId;
  }

  public ExecutionGraph getExecutionGraph() {
    return executionGraph;
  }

  public void setExecutionGraph(ExecutionGraph executionGraph) {
    this.executionGraph = executionGraph;
  }
}
