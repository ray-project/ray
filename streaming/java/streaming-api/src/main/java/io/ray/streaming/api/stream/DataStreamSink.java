package io.ray.streaming.api.stream;

import io.ray.streaming.operator.impl.SinkOperator;

/**
 * Represents a sink of the DataStream.
 *
 * @param <T> Type of the input data of this sink.
 */
public class DataStreamSink<T> extends StreamSink<T> {

  public DataStreamSink(DataStream<T> input, SinkOperator sinkOperator) {
    super(input, sinkOperator);
    this.streamingContext.addSink(this);
  }

  public DataStreamSink<T> setParallelism(int parallelism) {
    this.parallelism = parallelism;
    return this;
  }
}
