package org.ray.streaming.core.runtime.context;

import static org.ray.streaming.util.ConfigKey.STREAMING_MAX_BATCH_COUNT;

import java.util.Map;
import org.ray.streaming.core.graph.ExecutionTask;

/**
 * Use Ray to implement RuntimeContext.
 */
public class RayRuntimeContext implements RuntimeContext {

  private int taskId;
  private int taskIndex;
  private int parallelism;
  private Long batchId;
  private Map<String, Object> config;

  public RayRuntimeContext(ExecutionTask executionTask, Map<String, Object> config,
      int parallelism) {
    this.taskId = executionTask.getTaskId();
    this.config = config;
    this.taskIndex = executionTask.getTaskIndex();
    this.parallelism = parallelism;
  }

  @Override
  public int getTaskId() {
    return taskId;
  }

  @Override
  public int getTaskIndex() {
    return taskIndex;
  }

  @Override
  public int getParallelism() {
    return parallelism;
  }

  @Override
  public Long getBatchId() {
    return batchId;
  }

  @Override
  public Long getMaxBatch() {
    if (config.containsKey(STREAMING_MAX_BATCH_COUNT)) {
      return Long.valueOf(String.valueOf(config.get(STREAMING_MAX_BATCH_COUNT)));
    }
    return Long.MAX_VALUE;
  }

  public void setBatchId(Long batchId) {
    this.batchId = batchId;
  }
}
