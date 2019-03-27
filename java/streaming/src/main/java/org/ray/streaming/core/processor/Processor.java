package org.ray.streaming.core.processor;

import org.ray.streaming.api.collector.Collector;
import org.ray.streaming.core.runtime.context.RuntimeContext;
import java.io.Serializable;
import java.util.List;

public interface Processor<T> extends Serializable {

  void open(List<Collector> collectors, RuntimeContext runtimeContext);

  void process(T t);

  void close();
}
