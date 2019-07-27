package org.ray.streaming.core.processor;

import org.ray.streaming.message.Record;
import org.ray.streaming.operator.TwoInputOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TwoInputProcessor<T, O> extends StreamProcessor<Record, TwoInputOperator<T, O>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(TwoInputProcessor.class);

  // TODO(zhenxuanpan): Set leftStream and rightStream.
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
