package org.ray.streaming.python.stream;

import org.ray.streaming.api.context.StreamingContext;
import org.ray.streaming.api.stream.StreamSource;
import org.ray.streaming.python.descriptor.DescriptorFunction;
import org.ray.streaming.python.descriptor.DescriptorOperator;
import org.ray.streaming.python.descriptor.DescriptorPartition;

/**
 * Represents a source of the PythonStream.
 */
public class PythonStreamSource extends PythonDataStream implements StreamSource {

  public PythonStreamSource(StreamingContext streamingContext, DescriptorFunction sourceFunction) {
    super(streamingContext, DescriptorOperator.ofSource(sourceFunction));
    super.partition = DescriptorPartition.RoundRobinPartition;
  }

  public PythonStreamSource setParallelism(int parallelism) {
    this.parallelism = parallelism;
    return this;
  }

}
