package org.ray.streaming.runtime.worker.context;

import java.util.Map;
import org.ray.streaming.api.context.RuntimeContext;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import org.ray.streaming.util.Config;

/**
 * Use Ray to implement RuntimeContext.
 */
public class StreamingRuntimeContext implements RuntimeContext {
  private int globalTaskId;
  private int subTaskIndex;
  private int parallelism;
  private long batchId;
  private final long maxBatch;
  private Map<String, String> config;

  public StreamingRuntimeContext(ExecutionVertex executionVertex,
                                 Map<String, String> config,
                                 int parallelism) {
    this.globalTaskId = executionVertex.getVertexId();
    this.subTaskIndex = executionVertex.getVertexIndex();
    this.parallelism = parallelism;
    this.config = config;
    if (config.containsKey(Config.STREAMING_BATCH_MAX_COUNT)) {
      this.maxBatch = Long.valueOf(config.get(Config.STREAMING_BATCH_MAX_COUNT));
    } else {
      this.maxBatch = Long.MAX_VALUE;
    }
  }

  @Override
  public int getTaskId() {
    return globalTaskId;
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
    return batchId;
  }

  public void setBatchId(long batchId) {
    this.batchId = batchId;
  }

  @Override
  public Long getMaxBatch() {
    return maxBatch;
  }
}
