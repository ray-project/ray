package io.ray.streaming.runtime.core.processor;

import java.io.Serializable;
import java.util.List;
import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.context.RuntimeContext;

public interface Processor<T> extends Serializable {

  void open(List<Collector> collectors, RuntimeContext runtimeContext);

  void process(T t);

  void close();
}
