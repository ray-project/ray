package org.ray.streaming.python.stream;

import org.ray.streaming.api.Language;
import org.ray.streaming.api.stream.StreamSink;
import org.ray.streaming.python.PythonOperator;

/**
 * Represents a sink of the PythonStream.
 */
public class PythonStreamSink extends StreamSink implements PythonStream {
  public PythonStreamSink(PythonDataStream input, PythonOperator sinkOperator) {
    super(input, sinkOperator);
    getStreamingContext().addSink(this);
  }

  public PythonStreamSink setParallelism(int parallelism) {
    super.setParallelism(parallelism);
    return this;
  }

  @Override
  public Language getLanguage() {
    return Language.PYTHON;
  }

}
