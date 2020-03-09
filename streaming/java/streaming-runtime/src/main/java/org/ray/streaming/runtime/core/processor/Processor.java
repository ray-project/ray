package org.ray.streaming.runtime.core.processor;

import java.io.Serializable;
import java.util.List;
import org.ray.streaming.api.collector.Collector;
import org.ray.streaming.api.context.StreamRuntimeContext;

public interface Processor<T> extends Serializable {

  void open(List<Collector> collectors, StreamRuntimeContext streamRuntimeContext);

  void process(T t);

  void close();
}
