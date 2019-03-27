package com.ray.streaming.api.stream;

import com.ray.streaming.api.context.StreamingContext;
import com.ray.streaming.api.function.internal.CollectionSourceFunction;
import com.ray.streaming.operator.impl.SourceOperator;
import java.util.Collection;

/**
 * Represents a source of the DataStream.
 *
 * @param <T> The type of StreamSource data.
 */
public class StreamSource<T> extends DataStream<T> {

  public StreamSource(StreamingContext streamingContext, SourceOperator sourceOperator) {
    super(streamingContext, sourceOperator);
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
    return new StreamSource(context, new SourceOperator(new CollectionSourceFunction(values)));
  }

  public StreamSource<T> setParallelism(int parallelism) {
    this.parallelism = parallelism;
    return this;
  }
}
