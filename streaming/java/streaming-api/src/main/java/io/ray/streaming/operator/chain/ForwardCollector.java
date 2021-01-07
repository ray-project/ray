package io.ray.streaming.operator.chain;

import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.message.Record;
import io.ray.streaming.operator.OneInputOperator;

class ForwardCollector implements Collector<Record> {

  private final OneInputOperator succeedingOperator;

  ForwardCollector(OneInputOperator succeedingOperator) {
    this.succeedingOperator = succeedingOperator;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void collect(Record record) {
    try {
      succeedingOperator.processElement(record);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
