package org.ray.streaming.runtime.core.processor;

import java.io.Serializable;
import java.util.List;
import org.ray.streaming.api.collector.Collector;
import org.ray.streaming.api.context.RuntimeContext;

public interface Processor<T> extends Serializable {

  void open(List<Collector> collectors, RuntimeContext runtimeContext);

  void process(T t);

  void close();
}
