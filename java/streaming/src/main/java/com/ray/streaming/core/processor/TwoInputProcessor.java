package com.ray.streaming.core.processor;

import com.ray.streaming.message.Record;
import com.ray.streaming.operator.TwoInputOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulate the TwoInputProcessor for TwoInputOperator.
 * @param <T> The type of one input data.
 */
public class TwoInputProcessor<T, O> extends StreamProcessor<Record, TwoInputOperator<T, O>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(TwoInputProcessor.class);

  // TODO(zhenxuanpan): need add leftStream and rightStream set
  private String leftStream;
  private String rigthStream;

  public TwoInputProcessor(TwoInputOperator<T, O> operator) {
    super(operator);
  }

  @Override
  public void process(Record record) {
    try {
      if (record.getStream().equals(leftStream)) {
        this.operator.processElement(record, null);
      } else {
        this.operator.processElement(null, record);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    this.operator.close();
  }
}
