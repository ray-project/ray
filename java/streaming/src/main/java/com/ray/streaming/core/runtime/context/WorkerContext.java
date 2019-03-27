package com.ray.streaming.core.runtime.context;

import com.ray.streaming.core.graph.ExecutionGraph;
import java.io.Serializable;
import java.util.Map;

/**
 * Encapsulate the context information of a worker init.
 */
public class WorkerContext implements Serializable {

  private int taskId;
  private ExecutionGraph executionGraph;
  private Map<String, Object> config;

  public WorkerContext(int taskId, ExecutionGraph executionGraph, Map<String, Object> jobConfig) {
    this.taskId = taskId;
    this.executionGraph = executionGraph;
    this.config = config;
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

  public Map<String, Object> getConfig() {
    return config;
  }
}
