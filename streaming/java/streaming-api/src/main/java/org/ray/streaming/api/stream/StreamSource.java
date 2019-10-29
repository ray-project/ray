package org.ray.streaming.api.stream;

import java.util.Collection;
import org.ray.streaming.api.context.StreamingContext;
import org.ray.streaming.api.function.impl.SourceFunction;
import org.ray.streaming.api.function.internal.CollectionSourceFunction;
import org.ray.streaming.operator.impl.SourceOperator;

/**
 * Represents a source of the DataStream.
 *
 * @param <T> The type of StreamSource data.
 */
public class StreamSource<T> extends DataStream<T> {

  public StreamSource(StreamingContext streamingContext, SourceFunction<T> sourceFunction) {
    super(streamingContext, new SourceOperator<>(sourceFunction));
  }

  /**
   * Build a StreamSource source from a collection.
   *
   * @param context Stream context.
   * @param values A collection of values.
   * @param <T> The type of source data.
   * @return A StreamSource.
   */
  public static <T> StreamSource<T> buildSource(StreamingContext context, Collection<T> values) {
    return new StreamSource(context, new CollectionSourceFunction(values));
  }

  public StreamSource<T> setParallelism(int parallelism) {
    this.parallelism = parallelism;
    return this;
  }
}
