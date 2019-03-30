package org.ray.streaming.operator.impl;

import org.ray.streaming.api.function.impl.SinkFunction;
import org.ray.streaming.message.Record;
import org.ray.streaming.operator.OneInputOperator;
import org.ray.streaming.operator.StreamOperator;


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
