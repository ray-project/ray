package io.ray.streaming.runtime.worker.context;

import java.util.Map;

import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;

/**
 * Use Ray to implement RuntimeContext.
 */
public class StreamingRuntimeContext implements RuntimeContext {

  private int taskId;
  private int subTaskIndex;
  private int parallelism;
  private Map<String, String> config;

  public StreamingRuntimeContext(
      ExecutionVertex executionVertex,
      Map<String, String> config,
      int parallelism) {
    this.taskId = executionVertex.getId();
    this.subTaskIndex = executionVertex.getVertexIndex();
    this.parallelism = parallelism;
    this.config = config;
  }

  @Override
  public int getTaskId() {
    return taskId;
  }

  @Override
  public int getTaskIndex() {
    return subTaskIndex;
  }

  @Override
  public int getParallelism() {
    return parallelism;
  }

  @Override
  public Long getBatchId() {
    return null;
  }

  @Override
  public Long getMaxBatch() {
    return null;
  }

}
