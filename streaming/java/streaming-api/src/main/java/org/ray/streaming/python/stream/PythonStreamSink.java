package io.ray.streaming.python.stream;

import io.ray.streaming.api.stream.StreamSink;
import io.ray.streaming.python.PythonOperator;

/**
 * Represents a sink of the PythonStream.
 */
public class PythonStreamSink extends StreamSink implements PythonStream {
  public PythonStreamSink(PythonDataStream input, PythonOperator sinkOperator) {
    super(input, sinkOperator);
    this.streamingContext.addSink(this);
  }

  public PythonStreamSink setParallelism(int parallelism) {
    this.parallelism = parallelism;
    return this;
  }

}
