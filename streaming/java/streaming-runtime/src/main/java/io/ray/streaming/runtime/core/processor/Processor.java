package io.ray.streaming.runtime.core.processor;

import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.context.RuntimeContext;
import java.io.Serializable;
import java.util.List;

public interface Processor<T> extends Serializable {

  void open(List<Collector> collectors, RuntimeContext runtimeContext);

  void process(T t);

  void close();
}
