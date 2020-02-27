package org.ray.streaming.python.stream;

import org.ray.streaming.api.stream.StreamSink;
import org.ray.streaming.python.PythonOperator;

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
