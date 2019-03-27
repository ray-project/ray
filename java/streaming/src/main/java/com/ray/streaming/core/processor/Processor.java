package com.ray.streaming.core.processor;

import com.ray.streaming.api.collector.Collector;
import com.ray.streaming.core.runtime.context.RuntimeContext;
import java.io.Serializable;
import java.util.List;

public interface Processor<T> extends Serializable {

  void open(List<Collector> collectors, RuntimeContext runtimeContext);

  void process(T t);

  void close();
}
