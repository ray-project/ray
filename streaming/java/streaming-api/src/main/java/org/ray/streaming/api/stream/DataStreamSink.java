package org.ray.streaming.api.stream;

import org.ray.streaming.api.Language;
import org.ray.streaming.operator.impl.SinkOperator;

/**
 * Represents a sink of the DataStream.
 *
 * @param <T> Type of the input data of this sink.
 */
public class DataStreamSink<T> extends StreamSink<T> {

  public DataStreamSink(DataStream<T> input, SinkOperator sinkOperator) {
    super(input, sinkOperator);
    getStreamingContext().addSink(this);
  }

  public DataStreamSink<T> setParallelism(int parallelism) {
    super.setParallelism(parallelism);
    return this;
  }

  @Override
  public Language getLanguage() {
    return Language.JAVA;
  }
}
