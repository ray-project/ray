package io.ray.streaming.api.stream;


import io.ray.streaming.api.context.StreamingContext;
import io.ray.streaming.api.function.impl.FilterFunction;
import io.ray.streaming.api.function.impl.FlatMapFunction;
import io.ray.streaming.api.function.impl.KeyFunction;
import io.ray.streaming.api.function.impl.MapFunction;
import io.ray.streaming.api.function.impl.SinkFunction;
import io.ray.streaming.api.partition.Partition;
import io.ray.streaming.api.partition.impl.BroadcastPartition;
import io.ray.streaming.operator.StreamOperator;
import io.ray.streaming.operator.impl.FilterOperator;
import io.ray.streaming.operator.impl.FlatMapOperator;
import io.ray.streaming.operator.impl.KeyByOperator;
import io.ray.streaming.operator.impl.MapOperator;
import io.ray.streaming.operator.impl.SinkOperator;

/**
 * Represents a stream of data.
 *
 * This class defines all the streaming operations.
 *
 * @param <T> Type of data in the stream.
 */
public class DataStream<T> extends Stream<T> {

  public DataStream(StreamingContext streamingContext, StreamOperator streamOperator) {
    super(streamingContext, streamOperator);
  }

  public DataStream(DataStream input, StreamOperator streamOperator) {
    super(input, streamOperator);
  }

  /**
   * Apply a map function to this stream.
   *
   * @param mapFunction The map function.
   * @param <R> Type of data returned by the map function.
   * @return A new DataStream.
   */
  public <R> DataStream<R> map(MapFunction<T, R> mapFunction) {
    return new DataStream<>(this, new MapOperator(mapFunction));
  }

  /**
   * Apply a flat-map function to this stream.
   *
   * @param flatMapFunction The FlatMapFunction
   * @param <R> Type of data returned by the flatmap function.
   * @return A new DataStream
   */
  public <R> DataStream<R> flatMap(FlatMapFunction<T, R> flatMapFunction) {
    return new DataStream(this, new FlatMapOperator(flatMapFunction));
  }

  public DataStream<T> filter(FilterFunction<T> filterFunction) {
    return new DataStream<T>(this, new FilterOperator(filterFunction));
  }

  /**
   * Apply a union transformation to this stream, with another stream.
   *
   * @param other Another stream.
   * @return A new UnionStream.
   */
  public UnionStream<T> union(DataStream<T> other) {
    return new UnionStream(this, null, other);
  }

  /**
   * Apply a join transformation to this stream, with another stream.
   *
   * @param other Another stream.
   * @param <O> The type of the other stream data.
   * @param <R> The type of the data in the joined stream.
   * @return A new JoinStream.
   */
  public <O, R> JoinStream<T, O, R> join(DataStream<O> other) {
    return new JoinStream<>(this, other);
  }

  public <R> DataStream<R> process() {
    // TODO(zhenxuanpan): Need to add processFunction.
    return new DataStream(this, null);
  }

  /**
   * Apply a sink function and get a StreamSink.
   *
   * @param sinkFunction The sink function.
   * @return A new StreamSink.
   */
  public DataStreamSink<T> sink(SinkFunction<T> sinkFunction) {
    return new DataStreamSink<>(this, new SinkOperator(sinkFunction));
  }

  /**
   * Apply a key-by function to this stream.
   *
   * @param keyFunction the key function.
   * @param <K> The type of the key.
   * @return A new KeyDataStream.
   */
  public <K> KeyDataStream<K, T> keyBy(KeyFunction<T, K> keyFunction) {
    return new KeyDataStream<>(this, new KeyByOperator(keyFunction));
  }

  /**
   * Apply broadcast to this stream.
   *
   * @return This stream.
   */
  public DataStream<T> broadcast() {
    this.partition = new BroadcastPartition<>();
    return this;
  }

  /**
   * Apply a partition to this stream.
   *
   * @param partition The partitioning strategy.
   * @return This stream.
   */
  public DataStream<T> partitionBy(Partition<T> partition) {
    this.partition = partition;
    return this;
  }

  /**
   * Set parallelism to current transformation.
   *
   * @param parallelism The parallelism to set.
   * @return This stream.
   */
  public DataStream<T> setParallelism(int parallelism) {
    this.parallelism = parallelism;
    return this;
  }

}
