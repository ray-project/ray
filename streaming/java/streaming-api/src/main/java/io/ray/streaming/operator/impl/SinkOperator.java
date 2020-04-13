package io.ray.streaming.operator.impl;

import io.ray.streaming.api.function.impl.SinkFunction;
import io.ray.streaming.message.Record;
import io.ray.streaming.operator.OneInputOperator;
import io.ray.streaming.operator.StreamOperator;


public class SinkOperator<T> extends StreamOperator<SinkFunction<T>> implements
    OneInputOperator<T> {

  public SinkOperator(SinkFunction<T> sinkFunction) {
    super(sinkFunction);
  }

  @Override
  public void processElement(Record<T> record) throws Exception {
    this.function.sink(record.getValue());
  }
}
