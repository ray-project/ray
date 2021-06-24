package io.ray.streaming.operator.impl;

import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.api.function.impl.SourceFunction;
import io.ray.streaming.api.function.impl.SourceFunction.SourceContext;
import io.ray.streaming.message.Record;
import io.ray.streaming.operator.ChainStrategy;
import io.ray.streaming.operator.OperatorType;
import io.ray.streaming.operator.SourceOperator;
import io.ray.streaming.operator.StreamOperator;
import java.util.List;

public class SourceOperatorImpl<T> extends StreamOperator<SourceFunction<T>>
    implements SourceOperator {

  private SourceContextImpl sourceContext;

  public SourceOperatorImpl(SourceFunction<T> function) {
    super(function);
    setChainStrategy(ChainStrategy.HEAD);
  }

  @Override
  public void open(List<Collector> collectorList, RuntimeContext runtimeContext) {
    super.open(collectorList, runtimeContext);
    this.sourceContext = new SourceContextImpl(collectorList);
    this.function.init(runtimeContext.getParallelism(), runtimeContext.getTaskIndex());
  }

  @Override
  public void fetch() {
    try {
      this.function.fetch(this.sourceContext);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public SourceContext getSourceContext() {
    return sourceContext;
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
        collector.collect(new Record<>(t));
      }
    }
  }
}
