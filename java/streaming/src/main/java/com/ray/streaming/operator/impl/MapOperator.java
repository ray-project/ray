package com.ray.streaming.operator.impl;

import com.ray.streaming.api.function.impl.MapFunction;
import com.ray.streaming.message.Record;
import com.ray.streaming.operator.OneInputOperator;
import com.ray.streaming.operator.StreamOperator;


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
