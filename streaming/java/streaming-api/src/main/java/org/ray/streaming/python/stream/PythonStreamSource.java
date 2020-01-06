package org.ray.streaming.python.stream;

import org.ray.streaming.api.context.StreamingContext;
import org.ray.streaming.api.stream.StreamSource;
import org.ray.streaming.python.PythonOperator;
import org.ray.streaming.python.descriptor.DescriptorFunction;
import org.ray.streaming.python.descriptor.DescriptorFunction.PythonFunctionInterface;
import org.ray.streaming.python.descriptor.DescriptorPartition;

/**
 * Represents a source of the PythonStream.
 */
public class PythonStreamSource extends PythonDataStream implements StreamSource {

  private PythonStreamSource(StreamingContext streamingContext, DescriptorFunction sourceFunction) {
    super(streamingContext, new PythonOperator(sourceFunction));
    super.partition = DescriptorPartition.RoundRobinPartition;
  }

  public PythonStreamSource setParallelism(int parallelism) {
    this.parallelism = parallelism;
    return this;
  }

  public static PythonStreamSource from(StreamingContext streamingContext,
                                   DescriptorFunction sourceFunction) {
    sourceFunction.setFunctionInterface(PythonFunctionInterface.SOURCE_FUNCTION);
    return new PythonStreamSource(streamingContext, sourceFunction);
  }

}
