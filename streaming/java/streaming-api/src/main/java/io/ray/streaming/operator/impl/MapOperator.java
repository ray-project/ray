package io.ray.streaming.operator.impl;

import io.ray.streaming.api.function.impl.MapFunction;
import io.ray.streaming.message.Record;
import io.ray.streaming.operator.OneInputOperator;
import io.ray.streaming.operator.StreamOperator;


public class MapOperator<T, R> extends StreamOperator<MapFunction<T, R>> implements
    OneInputOperator<T> {

  public MapOperator(MapFunction<T, R> mapFunction) {
    super(mapFunction);
  }

  @Override
  public void processElement(Record<T> record) throws Exception {
    this.collect(new Record<R>(this.function.map(record.getValue())));
  }
}
