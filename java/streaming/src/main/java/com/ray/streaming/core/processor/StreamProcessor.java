package com.ray.streaming.core.processor;

import com.ray.streaming.api.collector.Collector;
import com.ray.streaming.core.runtime.context.RuntimeContext;
import com.ray.streaming.operator.Operator;
import java.util.List;

/**
 * Streaming Processor is a process unit for a operator
 * @param <T> The type of process data.
 * @param <P> The specific operator class.
 */
public abstract class StreamProcessor<T, P extends Operator> implements Processor<T> {

  protected List<Collector> collectors;
  protected RuntimeContext runtimeContext;
  protected P operator;

  public StreamProcessor(P operator) {
    this.operator = operator;
  }

  @Override
  public void open(List<Collector> collectors, RuntimeContext runtimeContext) {
    this.collectors = collectors;
    this.runtimeContext = runtimeContext;
    if (operator != null) {
      this.operator.open(collectors, runtimeContext);
    }
  }


}
