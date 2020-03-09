package org.ray.streaming.api.context;

/**
 * Encapsulate the runtime information of a streaming task.
 */
public interface StreamRuntimeContext {

  int getTaskId();

  int getTaskIndex();

  int getParallelism();

  Long getBatchId();

  Long getMaxBatch();

}
