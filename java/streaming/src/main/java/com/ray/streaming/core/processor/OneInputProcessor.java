package com.ray.streaming.core.processor;

import com.ray.streaming.message.Record;
import com.ray.streaming.operator.OneInputOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulate the OneInputProcessor for OneInputOperator.
 * @param <T> The type of one input data.
 */
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
