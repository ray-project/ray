package io.ray.streaming.api.stream;

import io.ray.streaming.api.function.impl.AggregateFunction;
import io.ray.streaming.api.function.impl.ReduceFunction;
import io.ray.streaming.api.partition.Partition;
import io.ray.streaming.api.partition.impl.KeyPartition;
import io.ray.streaming.operator.StreamOperator;
import io.ray.streaming.operator.impl.ReduceOperator;
import io.ray.streaming.python.stream.PythonDataStream;
import io.ray.streaming.python.stream.PythonKeyDataStream;

/**
 * Represents a DataStream returned by a key-by operation.
 *
 * @param <K> Type of the key.
 * @param <T> Type of the data.
 */
@SuppressWarnings("unchecked")
public class KeyDataStream<K, T> extends DataStream<T> {

  public KeyDataStream(DataStream<T> input, StreamOperator streamOperator) {
    super(input, streamOperator, (Partition<T>) new KeyPartition<K, T>());
  }

  /**
   * Create a java stream that reference passed python stream. Changes in new stream will be
   * reflected in referenced stream and vice versa
   */
  public KeyDataStream(PythonDataStream referencedStream) {
    super(referencedStream);
  }

  /**
   * Apply a reduce function to this stream.
   *
   * @param reduceFunction The reduce function. Returns A new DataStream.
   */
  public DataStream<T> reduce(ReduceFunction reduceFunction) {
    return new DataStream<>(this, new ReduceOperator(reduceFunction));
  }

  /**
   * Apply an aggregate Function to this stream.
   *
   * @param aggregateFunction The aggregate function
   * @param <A> The type of aggregated intermediate data.
   * @param <O> The type of result data. Returns A new DataStream.
   */
  public <A, O> DataStream<O> aggregate(AggregateFunction<T, A, O> aggregateFunction) {
    return new DataStream<>(this, null);
  }

  /**
   * Convert this stream as a python stream. The converted stream and this stream are the same
   * logical stream, which has same stream id. Changes in converted stream will be reflected in this
   * stream and vice versa.
   */
  public PythonKeyDataStream asPythonStream() {
    return new PythonKeyDataStream(this);
  }
}
