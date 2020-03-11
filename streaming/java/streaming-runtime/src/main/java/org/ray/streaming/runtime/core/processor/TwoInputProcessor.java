package org.ray.streaming.runtime.core.processor;

import org.ray.streaming.message.Record;
import org.ray.streaming.operator.TwoInputOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TwoInputProcessor<T, O> extends StreamProcessor<Record, TwoInputOperator<T, O>> {
  private static final Logger LOGGER = LoggerFactory.getLogger(TwoInputProcessor.class);

  private int leftStreamJobVertexId;
  private int rightStreamJobVertexId;

  public TwoInputProcessor(TwoInputOperator<T, O> operator) {
    super(operator);
  }

  @Override
  public void process(Record record) {
    try {
      if (record.getStream().equals(leftStreamJobVertexId)) {
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

  public int getLeftStreamJobVertexId() {
    return leftStreamJobVertexId;
  }

  public int getRightStreamJobVertexId() {
    return rightStreamJobVertexId;
  }

  public void setLeftStreamJobVertexId(int leftStreamJobVertexId) {
    this.leftStreamJobVertexId = leftStreamJobVertexId;
  }

  public void setRightStreamJobVertexId(int rightStreamJobVertexId) {
    this.rightStreamJobVertexId = rightStreamJobVertexId;
  }
}
