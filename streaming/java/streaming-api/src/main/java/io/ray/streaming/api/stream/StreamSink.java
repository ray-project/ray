package io.ray.streaming.api.stream;

import io.ray.streaming.operator.StreamOperator;

/**
 * Represents a sink of the Stream.
 *
 * @param <T> Type of the input data of this sink.
 */
public class StreamSink<T> extends Stream<T> {
  public StreamSink(Stream<T> inputStream, StreamOperator streamOperator) {
    super(inputStream, streamOperator);
  }
}
