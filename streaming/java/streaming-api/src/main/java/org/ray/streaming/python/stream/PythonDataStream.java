package org.ray.streaming.python.stream;

import org.ray.streaming.api.context.StreamingContext;
import org.ray.streaming.api.stream.Stream;
import org.ray.streaming.python.descriptor.DescriptorFunction;
import org.ray.streaming.python.descriptor.DescriptorOperator;
import org.ray.streaming.python.descriptor.DescriptorPartition;

public class PythonDataStream extends Stream implements PythonStream {

  public PythonDataStream(StreamingContext streamingContext,
                          DescriptorOperator descriptorOperator) {
    super(streamingContext, descriptorOperator);
  }

  public PythonDataStream(Stream inputStream, DescriptorOperator descriptorOperator) {
    super(inputStream, descriptorOperator);
  }

  /**
   * Apply a map function to this stream.
   *
   * @return A new PythonDataStream.
   */
  public PythonDataStream map(DescriptorFunction func) {
    return new PythonDataStream(this, DescriptorOperator.ofMap(func));
  }

  /**
   * Apply a flat-map function to this stream.
   *
   * @return A new PythonDataStream
   */
  public PythonDataStream flatMap(DescriptorFunction func) {
    return new PythonDataStream(this, DescriptorOperator.ofFlatMap(func));
  }

  /**
   * Apply a key-by function to this stream.
   *
   * @param func the key function.
   * @return A new KeyDataStream.
   */
  public PythonKeyDataStream keyBy(DescriptorFunction func) {
    return new PythonKeyDataStream(this, DescriptorOperator.ofKeyBy(func));
  }

  /**
   * Apply broadcast to this stream.
   *
   * @return This stream.
   */
  public PythonDataStream broadcast() {
    this.partition = DescriptorPartition.BroadcastPartition;
    return this;
  }

  /**
   * Apply a partition to this stream.
   *
   * @param partition The partitioning strategy.
   * @return This stream.
   */
  public PythonDataStream partitionBy(DescriptorPartition partition) {
    this.partition = partition;
    return this;
  }

  /**
   * Apply a sink function and get a StreamSink.
   *
   * @param func The sink function.
   * @return A new StreamSink.
   */
  public PythonStreamSink sink(DescriptorFunction func) {
    return new PythonStreamSink(this, DescriptorOperator.ofSink(func));
  }

  /**
   * Set parallelism to current transformation.
   *
   * @param parallelism The parallelism to set.
   * @return This stream.
   */
  public PythonDataStream setParallelism(int parallelism) {
    this.parallelism = parallelism;
    return this;
  }

}
