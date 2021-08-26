package io.ray.streaming.runtime.core.processor;

import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.api.function.Function;
import java.io.Serializable;
import java.util.List;

public interface Processor<T> extends Serializable {

  void open(List<Collector> collectors, RuntimeContext runtimeContext);

  void process(T t);

  /** See {@link Function#saveCheckpoint()}. */
  Serializable saveCheckpoint();

  /** See {@link Function#loadCheckpoint(Serializable)}. */
  void loadCheckpoint(Serializable checkpointObject);

  void close();
}
