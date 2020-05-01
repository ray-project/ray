package io.ray.streaming.api.stream;

import io.ray.streaming.api.Language;
import io.ray.streaming.operator.impl.SinkOperator;

/**
 * Represents a sink of the DataStream.
 *
 * @param <T> Type of the input data of this sink.
 */
public class DataStreamSink<T> extends StreamSink<T> {

  public DataStreamSink(DataStream input, SinkOperator sinkOperator) {
    super(input, sinkOperator);
    getStreamingContext().addSink(this);
  }

  @Override
  public Language getLanguage() {
    return Language.JAVA;
  }
}
