package org.ray.streaming.runtime.worker.context;

import java.util.Map;
import org.ray.streaming.api.context.RuntimeContext;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;

/**
 * Use Ray to implement RuntimeContext.
 */
public class StreamingRuntimeContext implements RuntimeContext {
  private int taskId;
  private int subTaskIndex;
  private int parallelism;
  private Map<String, String> config;

  public StreamingRuntimeContext(ExecutionVertex executionVertex,
                                 Map<String, String> config,
                                 int parallelism) {
    this.taskId = executionVertex.getVertexId();
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

}
