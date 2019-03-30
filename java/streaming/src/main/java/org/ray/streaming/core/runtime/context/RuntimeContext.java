package org.ray.streaming.core.runtime.context;

/**
 * Encapsulate the runtime information of a streaming task.
 */
public interface RuntimeContext {

  int getTaskId();

  int getTaskIndex();

  int getParallelism();

  Long getBatchId();

  Long getMaxBatch();


}
