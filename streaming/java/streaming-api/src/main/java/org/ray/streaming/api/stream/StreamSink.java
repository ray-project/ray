package org.ray.streaming.api.stream;

import org.ray.streaming.operator.impl.SinkOperator;

/**
 * Represents a sink of the DataStream.
 *
 * @param <T> Type of the input data of this sink.
 */
public class StreamSink<T> extends Stream<T> {

  public StreamSink(DataStream<T> input, SinkOperator sinkOperator) {
    super(input, sinkOperator);
    this.streamingContext.addSink(this);
  }

  public StreamSink<T> setParallelism(int parallelism) {
    this.parallelism = parallelism;
    return this;
  }
}
