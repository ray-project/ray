package com.ray.streaming.operator.impl;

import com.ray.streaming.api.collector.Collector;
import com.ray.streaming.api.function.impl.SourceFunction;
import com.ray.streaming.api.function.impl.SourceFunction.SourceContext;
import com.ray.streaming.core.runtime.context.RuntimeContext;
import com.ray.streaming.message.Record;
import com.ray.streaming.operator.OperatorType;
import com.ray.streaming.operator.StreamOperator;
import java.util.List;

public class SourceOperator<T> extends StreamOperator<SourceFunction<T>> {

  private SourceContextImpl sourceContext;

  public SourceOperator(SourceFunction<T> function) {
    super(function);
  }

  @Override
  public void open(List<Collector> collectorList, RuntimeContext runtimeContext) {
    super.open(collectorList, runtimeContext);
    this.sourceContext = new SourceContextImpl(collectorList);
  }

  public void process(Long batchId) {
    try {
      this.sourceContext.setBatchId(batchId);
      this.function.fetch(batchId, this.sourceContext);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  @Override
  public OperatorType getOpType() {
    return OperatorType.SOURCE;
  }

  class SourceContextImpl implements SourceContext<T> {

    private long batchId;
    private List<Collector> collectors;

    public SourceContextImpl(List<Collector> collectors) {
      this.collectors = collectors;
    }

    @Override
    public void collect(T t) throws Exception {
      for (Collector collector : collectors) {
        collector.collect(new Record(batchId, t));
      }
    }

    private void setBatchId(long batchId) {
      this.batchId = batchId;
    }
  }
}
