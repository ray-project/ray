package org.ray.streaming.python.stream;

import org.ray.streaming.api.context.StreamingContext;
import org.ray.streaming.api.stream.StreamSource;
import org.ray.streaming.python.PythonFunction;
import org.ray.streaming.python.PythonFunction.FunctionInterface;
import org.ray.streaming.python.PythonOperator;
import org.ray.streaming.python.PythonPartition;

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
