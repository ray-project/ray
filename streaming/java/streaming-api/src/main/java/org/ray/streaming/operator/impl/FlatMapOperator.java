package org.ray.streaming.operator.impl;

import java.util.List;
import org.ray.streaming.api.collector.CollectionCollector;
import org.ray.streaming.api.collector.Collector;
import org.ray.streaming.api.context.RuntimeContext;
import org.ray.streaming.api.function.impl.FlatMapFunction;
import org.ray.streaming.message.Record;
import org.ray.streaming.operator.OneInputOperator;
import org.ray.streaming.operator.StreamOperator;

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
