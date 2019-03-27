package com.ray.streaming.core.runtime.context;

import com.ray.streaming.core.graph.ExecutionTask;

/**
 * Use Ray to implement RuntimeContext.
 */
public class RayRuntimeContext implements RuntimeContext {

  private int taskId;
  private int taskIndex;
  private int parallelism;
  private Long batchId;

  public RayRuntimeContext(ExecutionTask executionTask, int parallelism) {
    this.taskId = executionTask.getTaskId();
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

  public void setBatchId(Long batchId) {
    this.batchId = batchId;
  }
}
