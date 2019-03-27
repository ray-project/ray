package com.ray.streaming.operator.impl;

import com.ray.streaming.api.function.impl.SinkFunction;
import com.ray.streaming.message.Record;
import com.ray.streaming.operator.OneInputOperator;
import com.ray.streaming.operator.StreamOperator;


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
