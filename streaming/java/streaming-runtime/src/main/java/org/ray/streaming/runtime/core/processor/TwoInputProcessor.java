package io.ray.streaming.runtime.core.processor;

import io.ray.streaming.message.Record;
import io.ray.streaming.operator.TwoInputOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TwoInputProcessor<T, O> extends StreamProcessor<Record, TwoInputOperator<T, O>> {
  private static final Logger LOGGER = LoggerFactory.getLogger(TwoInputProcessor.class);

  private String leftStream;
  private String rightStream;

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

  public String getLeftStream() {
    return leftStream;
  }

  public void setLeftStream(String leftStream) {
    this.leftStream = leftStream;
  }

  public String getRightStream() {
    return rightStream;
  }

  public void setRightStream(String rightStream) {
    this.rightStream = rightStream;
  }
}
