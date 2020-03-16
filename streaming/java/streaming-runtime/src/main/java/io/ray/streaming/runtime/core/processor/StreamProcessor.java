package io.ray.streaming.runtime.core.processor;

import io.ray.streaming.api.collector.Collector;
import java.util.List;
import org.ray.streaming.api.context.StreamRuntimeContext;
import org.ray.streaming.operator.Operator;
import org.ray.streaming.state.backend.KeyStateBackend;
import org.ray.streaming.state.backend.impl.MemoryStateBackend;
import org.ray.streaming.state.config.ConfigKey;
import org.ray.streaming.state.keystate.KeyGroup;
import org.ray.streaming.state.keystate.KeyGroupAssignment;
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
  protected StreamRuntimeContext runtimeContext;
  protected P operator;
  private KeyStateBackend keyStateBackend;

  public StreamProcessor(P operator) {
    this.operator = operator;
  }

  @Override
  public void open(List<Collector> collectors, StreamRuntimeContext runtimeContext) {
    this.collectors = collectors;
    this.runtimeContext = runtimeContext;
    int maxParallelism = Integer.valueOf(runtimeContext.getConfig().getOrDefault(
        ConfigKey.JOB_MAX_PARALLEL, "1024"));
    KeyGroup subProcessorKeyGroup = KeyGroupAssignment
        .getKeyGroup(maxParallelism, runtimeContext.getParallelism(),
            runtimeContext.getTaskIndex());
    this.keyStateBackend = new KeyStateBackend(maxParallelism, subProcessorKeyGroup,
        new MemoryStateBackend(runtimeContext.getConfig()));
    runtimeContext.setKeyStateBackend(keyStateBackend);
    if (operator != null) {
      this.operator.open(collectors, runtimeContext);
    }
    LOGGER.info("opened {}", this);
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }
}
