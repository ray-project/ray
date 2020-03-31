package io.ray.streaming.python.stream;

import io.ray.streaming.api.context.StreamingContext;
import io.ray.streaming.api.stream.StreamSource;
import io.ray.streaming.python.PythonFunction;
import io.ray.streaming.python.PythonFunction.FunctionInterface;
import io.ray.streaming.python.PythonOperator;
import io.ray.streaming.python.PythonPartition;

/**
 * Represents a source of the PythonStream.
 */
public class PythonStreamSource extends PythonDataStream implements StreamSource {

  private PythonStreamSource(StreamingContext streamingContext, PythonFunction sourceFunction) {
    super(streamingContext, new PythonOperator(sourceFunction));
    super.partition = PythonPartition.RoundRobinPartition;
  }

  public PythonStreamSource setParallelism(int parallelism) {
    this.parallelism = parallelism;
    return this;
  }

  public static PythonStreamSource from(StreamingContext streamingContext,
                                   PythonFunction sourceFunction) {
    sourceFunction.setFunctionInterface(FunctionInterface.SOURCE_FUNCTION);
    return new PythonStreamSource(streamingContext, sourceFunction);
  }

}
