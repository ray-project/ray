package io.ray.streaming.api.stream;

import io.ray.streaming.api.context.StreamingContext;
import io.ray.streaming.api.function.impl.SourceFunction;
import io.ray.streaming.api.function.internal.CollectionSourceFunction;
import io.ray.streaming.api.partition.impl.RoundRobinPartition;
import io.ray.streaming.operator.impl.SourceOperator;
import java.util.Collection;

/**
 * Represents a source of the DataStream.
 *
 * @param <T> The type of StreamSource data.
 */
public class DataStreamSource<T> extends DataStream<T> implements StreamSource<T> {

  public DataStreamSource(StreamingContext streamingContext, SourceFunction<T> sourceFunction) {
    super(streamingContext, new SourceOperator<>(sourceFunction));
    super.partition = new RoundRobinPartition<>();
  }

  /**
   * Build a DataStreamSource source from a collection.
   *
   * @param context Stream context.
   * @param values  A collection of values.
   * @param <T>     The type of source data.
   * @return A DataStreamSource.
   */
  public static <T> DataStreamSource<T> buildSource(
      StreamingContext context, Collection<T> values) {
    return new DataStreamSource(context, new CollectionSourceFunction(values));
  }

  @Override
  public DataStreamSource<T> setParallelism(int parallelism) {
    this.parallelism = parallelism;
    return this;
  }
}
