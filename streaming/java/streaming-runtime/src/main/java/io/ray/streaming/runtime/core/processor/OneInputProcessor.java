package io.ray.streaming.runtime.core.processor;

import io.ray.streaming.message.Record;
import io.ray.streaming.operator.OneInputOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OneInputProcessor<T> extends StreamProcessor<Record<T>, OneInputOperator<T>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(OneInputProcessor.class);

  public OneInputProcessor(OneInputOperator<T> operator) {
    super(operator);
  }

  @Override
  public void process(Record<T> record) {
    try {
      this.operator.processElement(record);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    this.operator.close();
  }
}
