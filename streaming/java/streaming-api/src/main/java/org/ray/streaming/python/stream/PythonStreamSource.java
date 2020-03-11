package org.ray.streaming.python.stream;

import org.ray.streaming.api.context.StreamContext;
import org.ray.streaming.api.stream.StreamSource;
import org.ray.streaming.python.PythonFunction;
import org.ray.streaming.python.PythonFunction.FunctionInterface;
import org.ray.streaming.python.PythonOperator;
import org.ray.streaming.python.PythonPartition;

/**
 * Represents a source of the PythonStream.
 */
public class PythonStreamSource extends PythonDataStream implements StreamSource {

  private PythonStreamSource(StreamContext streamContext, PythonFunction sourceFunction) {
    super(streamContext, new PythonOperator(sourceFunction));
    super.partition = PythonPartition.RoundRobinPartition;
  }

  public PythonStreamSource setParallelism(int parallelism) {
    this.parallelism = parallelism;
    return this;
  }

  public static PythonStreamSource from(StreamContext streamContext,
                                   PythonFunction sourceFunction) {
    sourceFunction.setFunctionInterface(FunctionInterface.SOURCE_FUNCTION);
    return new PythonStreamSource(streamContext, sourceFunction);
  }

}
