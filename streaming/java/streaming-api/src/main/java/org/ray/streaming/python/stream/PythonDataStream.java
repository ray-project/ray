package org.ray.streaming.python.stream;

import org.ray.streaming.api.context.StreamingContext;
import org.ray.streaming.api.stream.Stream;
import org.ray.streaming.python.PythonOperator;
import org.ray.streaming.python.descriptor.DescriptorFunction;
import org.ray.streaming.python.descriptor.DescriptorFunction.PythonFunctionInterface;
import org.ray.streaming.python.descriptor.DescriptorPartition;

/**
 * Represents a stream of data whose transformations will be executed in python.
 */
public class PythonDataStream extends Stream implements PythonStream {

  protected PythonDataStream(StreamingContext streamingContext,
                             PythonOperator pythonOperator) {
    super(streamingContext, pythonOperator);
  }

  protected PythonDataStream(Stream inputStream, PythonOperator pythonOperator) {
    super(inputStream, pythonOperator);
  }

  /**
   * Apply a map function to this stream.
   *
   * @param func The python MapFunction.
   * @return A new PythonDataStream.
   */
  public PythonDataStream map(DescriptorFunction func) {
    func.setPythonFunctionInterface(PythonFunctionInterface.MAP_FUNCTION);
    return new PythonDataStream(this, new PythonOperator(func));
  }

  /**
   * Apply a flat-map function to this stream.
   *
   * @param func The python FlapMapFunction.
   * @return A new PythonDataStream
   */
  public PythonDataStream flatMap(DescriptorFunction func) {
    func.setPythonFunctionInterface(PythonFunctionInterface.FLAT_MAP_FUNCTION);
    return new PythonDataStream(this, new PythonOperator(func));
  }

  /**
   * Apply a sink function and get a StreamSink.
   *
   * @param func The python SinkFunction.
   * @return A new StreamSink.
   */
  public PythonStreamSink sink(DescriptorFunction func) {
    func.setPythonFunctionInterface(DescriptorFunction.PythonFunctionInterface.SINK_FUNCTION);
    return new PythonStreamSink(this, new PythonOperator(func));
  }

  /**
   * Apply a key-by function to this stream.
   *
   * @param func the  python keyFunction.
   * @return A new KeyDataStream.
   */
  public PythonKeyDataStream keyBy(DescriptorFunction func) {
    func.setPythonFunctionInterface(PythonFunctionInterface.KEY_FUNCTION);
    return new PythonKeyDataStream(this, new PythonOperator(func));
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
