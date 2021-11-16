package io.ray.streaming.operator.impl;

import io.ray.streaming.api.collector.CollectionCollector;
import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.api.function.impl.FlatMapFunction;
import io.ray.streaming.message.Record;
import io.ray.streaming.operator.OneInputOperator;
import io.ray.streaming.operator.StreamOperator;
import java.util.List;

public class FlatMapOperator<T, R> extends StreamOperator<FlatMapFunction<T, R>>
    implements OneInputOperator<T> {

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
