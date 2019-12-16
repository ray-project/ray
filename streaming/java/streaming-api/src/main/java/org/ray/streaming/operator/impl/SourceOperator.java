package org.ray.streaming.operator.impl;

import java.util.List;
import org.ray.streaming.api.collector.Collector;
import org.ray.streaming.api.context.RuntimeContext;
import org.ray.streaming.api.function.impl.SourceFunction;
import org.ray.streaming.api.function.impl.SourceFunction.SourceContext;
import org.ray.streaming.message.Record;
import org.ray.streaming.operator.OperatorType;
import org.ray.streaming.operator.StreamOperator;

public class SourceOperator<T> extends StreamOperator<SourceFunction<T>> {

  private SourceContextImpl sourceContext;

  public SourceOperator(SourceFunction<T> function) {
    super(function);
  }

  @Override
  public void open(List<Collector> collectorList, RuntimeContext runtimeContext) {
    super.open(collectorList, runtimeContext);
    this.sourceContext = new SourceContextImpl(collectorList);
    this.function.init(runtimeContext.getParallelism(), runtimeContext.getTaskIndex());
  }

  public void run() {
    try {
      this.function.run(this.sourceContext);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public OperatorType getOpType() {
    return OperatorType.SOURCE;
  }

  class SourceContextImpl implements SourceContext<T> {
    private List<Collector> collectors;

    public SourceContextImpl(List<Collector> collectors) {
      this.collectors = collectors;
    }

    @Override
    public void collect(T t) throws Exception {
      for (Collector collector : collectors) {
        collector.collect(new Record(t));
      }
    }

  }
}
