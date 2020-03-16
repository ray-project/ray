package io.ray.streaming.runtime.core.processor;

import io.ray.streaming.api.collector.Collector;
import java.io.Serializable;
import java.util.List;
import org.ray.streaming.api.context.StreamRuntimeContext;

public interface Processor<T> extends Serializable {

  void open(List<Collector> collectors, StreamRuntimeContext runtimeContext);

  void process(T t);

  void close();
}
