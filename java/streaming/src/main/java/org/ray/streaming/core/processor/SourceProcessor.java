package org.ray.streaming.core.processor;

import org.ray.streaming.operator.impl.SourceOperator;

/**
 * The processor for the stream sources, containing a SourceOperator.
 *
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
