package org.ray.streaming.runtime.worker.context;

import java.io.Serializable;
import java.util.Map;
import org.ray.streaming.runtime.core.graph.ExecutionGraph;

/**
 * Encapsulate the context information for worker initialization.
 */
public class WorkerContext implements Serializable {

  private int taskId;
  private ExecutionGraph executionGraph;
  private Map<String, String> config;

  public WorkerContext(int taskId, ExecutionGraph executionGraph, Map<String, String> jobConfig) {
    this.taskId = taskId;
    this.executionGraph = executionGraph;
    this.config = jobConfig;
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

  public Map<String, String> getConfig() {
    return config;
  }
}
