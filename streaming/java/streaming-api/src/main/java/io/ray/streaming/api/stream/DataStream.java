package io.ray.streaming.api.stream;

import io.ray.streaming.api.Language;
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
import io.ray.streaming.python.stream.PythonDataStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Represents a stream of data.
 *
 * <p>This class defines all the streaming operations.
 *
 * @param <T> Type of data in the stream.
 */
public class DataStream<T> extends Stream<DataStream<T>, T> {

  public DataStream(StreamingContext streamingContext, StreamOperator streamOperator) {
    super(streamingContext, streamOperator);
  }

  public DataStream(
      StreamingContext streamingContext, StreamOperator streamOperator, Partition<T> partition) {
    super(streamingContext, streamOperator, partition);
  }

  public <R> DataStream(DataStream<R> input, StreamOperator streamOperator) {
    super(input, streamOperator);
  }

  public <R> DataStream(
      DataStream<R> input, StreamOperator streamOperator, Partition<T> partition) {
    super(input, streamOperator, partition);
  }

  /**
   * Create a java stream that reference passed python stream. Changes in new stream will be
   * reflected in referenced stream and vice versa
   */
  public DataStream(PythonDataStream referencedStream) {
    super(referencedStream);
  }

  /**
   * Apply a map function to this stream.
   *
   * @param mapFunction The map function.
   * @param <R> Type of data returned by the map function. Returns A new DataStream.
   */
  public <R> DataStream<R> map(MapFunction<T, R> mapFunction) {
    return new DataStream<>(this, new MapOperator<>(mapFunction));
  }

  /**
   * Apply a flat-map function to this stream.
   *
   * @param flatMapFunction The FlatMapFunction
   * @param <R> Type of data returned by the flatmap function. Returns A new DataStream
   */
  public <R> DataStream<R> flatMap(FlatMapFunction<T, R> flatMapFunction) {
    return new DataStream<>(this, new FlatMapOperator<>(flatMapFunction));
  }

  public DataStream<T> filter(FilterFunction<T> filterFunction) {
    return new DataStream<>(this, new FilterOperator<>(filterFunction));
  }

  /**
   * Apply union transformations to this stream by merging {@link DataStream} outputs of the same
   * type with each other.
   *
   * @param stream The DataStream to union output with.
   * @param others The other DataStreams to union output with. Returns A new UnionStream.
   */
  @SafeVarargs
  public final DataStream<T> union(DataStream<T> stream, DataStream<T>... others) {
    List<DataStream<T>> streams = new ArrayList<>();
    streams.add(stream);
    streams.addAll(Arrays.asList(others));
    return union(streams);
  }

  /**
   * Apply union transformations to this stream by merging {@link DataStream} outputs of the same
   * type with each other.
   *
   * @param streams The DataStreams to union output with. Returns A new UnionStream.
   */
  public final DataStream<T> union(List<DataStream<T>> streams) {
    if (this instanceof UnionStream) {
      UnionStream<T> unionStream = (UnionStream<T>) this;
      streams.forEach(unionStream::addStream);
      return unionStream;
    } else {
      return new UnionStream<>(this, streams);
    }
  }

  /**
   * Apply a join transformation to this stream, with another stream.
   *
   * @param other Another stream.
   * @param <O> The type of the other stream data.
   * @param <R> The type of the data in the joined stream. Returns A new JoinStream.
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
   * @param sinkFunction The sink function. Returns A new StreamSink.
   */
  public DataStreamSink<T> sink(SinkFunction<T> sinkFunction) {
    return new DataStreamSink<>(this, new SinkOperator<>(sinkFunction));
  }

  /**
   * Apply a key-by function to this stream.
   *
   * @param keyFunction the key function.
   * @param <K> The type of the key. Returns A new KeyDataStream.
   */
  public <K> KeyDataStream<K, T> keyBy(KeyFunction<T, K> keyFunction) {
    checkPartitionCall();
    return new KeyDataStream<>(this, new KeyByOperator<>(keyFunction));
  }

  /**
   * Apply broadcast to this stream.
   *
   * <p>Returns This stream.
   */
  public DataStream<T> broadcast() {
    checkPartitionCall();
    return setPartition(new BroadcastPartition<>());
  }

  /**
   * Apply a partition to this stream.
   *
   * @param partition The partitioning strategy. Returns This stream.
   */
  public DataStream<T> partitionBy(Partition<T> partition) {
    checkPartitionCall();
    return setPartition(partition);
  }

  /**
   * If parent stream is a python stream, we can't call partition related methods in the java
   * stream.
   */
  private void checkPartitionCall() {
    if (getInputStream() != null && getInputStream().getLanguage() == Language.PYTHON) {
      throw new RuntimeException(
          "Partition related methods can't be called on a "
              + "java stream if parent stream is a python stream.");
    }
  }

  /**
   * Convert this stream as a python stream. The converted stream and this stream are the same
   * logical stream, which has same stream id. Changes in converted stream will be reflected in this
   * stream and vice versa.
   */
  public PythonDataStream asPythonStream() {
    return new PythonDataStream(this);
  }

  @Override
  public Language getLanguage() {
    return Language.JAVA;
  }
}
