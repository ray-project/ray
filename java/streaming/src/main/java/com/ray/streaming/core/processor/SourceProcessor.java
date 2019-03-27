package com.ray.streaming.core.processor;

import com.ray.streaming.operator.impl.SourceOperator;

/**
 * Encapsulate the SourceProcessor for SourceProcessor.
 * @param <T> The type of source data.
 */
public class SourceProcessor<T> extends StreamProcessor<Long, SourceOperator<T>> {

  public SourceProcessor(SourceOperator<T> operator) {
    super(operator);
  }

  @Override
  public void process(Long batchId) {
    this.operator.process(batchId);
  }

  @Override
  public void close() {

  }
}
