package org.ray.streaming.runtime.core.processor;

import java.util.List;
import org.ray.streaming.api.collector.Collector;
import org.ray.streaming.api.context.StreamRuntimeContext;
import org.ray.streaming.operator.Operator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StreamingProcessor is a process unit for a operator.
 *
 * @param <T> The type of process data.
 * @param <P> Type of the specific operator class.
 */
public abstract class StreamProcessor<T, P extends Operator> implements Processor<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamProcessor.class);

  protected List<Collector> collectors;
  protected StreamRuntimeContext streamRuntimeContext;
  protected P operator;

  public StreamProcessor(P operator) {
    this.operator = operator;
  }

  @Override
  public void open(List<Collector> collectors, StreamRuntimeContext streamRuntimeContext) {
    this.collectors = collectors;
    this.streamRuntimeContext = streamRuntimeContext;
    if (operator != null) {
      this.operator.open(collectors, streamRuntimeContext);
    }
    LOGGER.info("opened {}", this);
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }
}
