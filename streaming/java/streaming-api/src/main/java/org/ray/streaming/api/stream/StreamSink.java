package org.ray.streaming.api.stream;

import org.ray.streaming.operator.StreamOperator;

/**
 * Represents a sink of the Stream.
 *
 * @param <T> Type of the input data of this sink.
 */
public abstract class StreamSink<T> extends Stream<StreamSink<T>, T> {
  public StreamSink(Stream inputStream, StreamOperator streamOperator) {
    super(inputStream, streamOperator);
  }
}
