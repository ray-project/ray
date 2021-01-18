package io.ray.streaming.python.stream;

import io.ray.streaming.api.context.StreamingContext;
import io.ray.streaming.api.stream.StreamSource;
import io.ray.streaming.operator.ChainStrategy;
import io.ray.streaming.python.PythonFunction;
import io.ray.streaming.python.PythonFunction.FunctionInterface;
import io.ray.streaming.python.PythonOperator;

/** Represents a source of the PythonStream. */
public class PythonStreamSource extends PythonDataStream implements StreamSource {

  private PythonStreamSource(StreamingContext streamingContext, PythonFunction sourceFunction) {
    super(streamingContext, new PythonOperator(sourceFunction));
    withChainStrategy(ChainStrategy.HEAD);
  }

  public static PythonStreamSource from(
      StreamingContext streamingContext, PythonFunction sourceFunction) {
    sourceFunction.setFunctionInterface(FunctionInterface.SOURCE_FUNCTION);
    return new PythonStreamSource(streamingContext, sourceFunction);
  }
}
