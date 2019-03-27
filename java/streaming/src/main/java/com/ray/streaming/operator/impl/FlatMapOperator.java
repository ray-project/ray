package com.ray.streaming.operator.impl;

import com.ray.streaming.api.collector.Collector;
import com.ray.streaming.api.function.impl.FlatMapFunction;
import com.ray.streaming.core.runtime.collector.CollectionCollector;
import com.ray.streaming.core.runtime.context.RuntimeContext;
import com.ray.streaming.message.Record;
import com.ray.streaming.operator.OneInputOperator;
import com.ray.streaming.operator.StreamOperator;
import java.util.List;

public class FlatMapOperator<T, R> extends StreamOperator<FlatMapFunction<T, R>> implements
    OneInputOperator<T> {

  private CollectionCollector collectionCollector;

  public FlatMapOperator(FlatMapFunction<T, R> flatMapFunction) {
    super(flatMapFunction);
  }

  @Override
  public void open(List<Collector> collectorList, RuntimeContext runtimeContext) {
    super.open(collectorList, runtimeContext);
    this.collectionCollector = new CollectionCollector(collectorList);
  }

  @Override
  public void processElement(Record<T> record) throws Exception {
    this.function.flatMap(record.getValue(), (Collector<R>) collectionCollector);

  }
}
