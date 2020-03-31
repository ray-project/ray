package io.ray.streaming.api.stream;

import io.ray.streaming.api.function.impl.AggregateFunction;
import io.ray.streaming.api.function.impl.ReduceFunction;
import io.ray.streaming.api.partition.impl.KeyPartition;
import io.ray.streaming.operator.StreamOperator;
import io.ray.streaming.operator.impl.ReduceOperator;

/**
 * Represents a DataStream returned by a key-by operation.
 *
 * @param <K> Type of the key.
 * @param <T> Type of the data.
 */
public class KeyDataStream<K, T> extends DataStream<T> {

  public KeyDataStream(DataStream<T> input, StreamOperator streamOperator) {
    super(input, streamOperator);
    this.partition = new KeyPartition();
  }

  /**
   * Apply a reduce function to this stream.
   *
   * @param reduceFunction The reduce function.
   * @return A new DataStream.
   */
  public DataStream<T> reduce(ReduceFunction reduceFunction) {
    return new DataStream<>(this, new ReduceOperator(reduceFunction));
  }

  /**
   * Apply an aggregate Function to this stream.
   *
   * @param aggregateFunction The aggregate function
   * @param <A> The type of aggregated intermediate data.
   * @param <O> The type of result data.
   * @return A new DataStream.
   */
  public <A, O> DataStream<O> aggregate(AggregateFunction<T, A, O> aggregateFunction) {
    return new DataStream<>(this, null);
  }

  public KeyDataStream<K, T> setParallelism(int parallelism) {
    this.parallelism = parallelism;
    return this;
  }
}
