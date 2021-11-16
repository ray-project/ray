package io.ray.streaming.python.stream;

import io.ray.streaming.api.Language;
import io.ray.streaming.api.stream.StreamSink;
import io.ray.streaming.python.PythonOperator;

/** Represents a sink of the PythonStream. */
public class PythonStreamSink extends StreamSink implements PythonStream {

  public PythonStreamSink(PythonDataStream input, PythonOperator sinkOperator) {
    super(input, sinkOperator);
    getStreamingContext().addSink(this);
  }

  @Override
  public Language getLanguage() {
    return Language.PYTHON;
  }
}
