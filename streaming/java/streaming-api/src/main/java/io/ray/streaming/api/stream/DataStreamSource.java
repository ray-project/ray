package io.ray.streaming.api.stream;

import io.ray.streaming.api.context.StreamingContext;
import io.ray.streaming.api.function.impl.SourceFunction;
import io.ray.streaming.api.function.internal.CollectionSourceFunction;
import io.ray.streaming.operator.impl.SourceOperatorImpl;
import java.util.Collection;

/**
 * Represents a source of the DataStream.
 *
 * @param <T> The type of StreamSource data.
 */
public class DataStreamSource<T> extends DataStream<T> implements StreamSource<T> {

  private DataStreamSource(StreamingContext streamingContext, SourceFunction<T> sourceFunction) {
    super(streamingContext, new SourceOperatorImpl<>(sourceFunction));
  }

  public static <T> DataStreamSource<T> fromSource(
      StreamingContext context, SourceFunction<T> sourceFunction) {
    return new DataStreamSource<>(context, sourceFunction);
  }

  /**
   * Build a DataStreamSource source from a collection.
   *
   * @param context Stream context.
   * @param values A collection of values.
   * @param <T> The type of source data. Returns A DataStreamSource.
   */
  public static <T> DataStreamSource<T> fromCollection(
      StreamingContext context, Collection<T> values) {
    return new DataStreamSource<>(context, new CollectionSourceFunction<>(values));
  }
}
