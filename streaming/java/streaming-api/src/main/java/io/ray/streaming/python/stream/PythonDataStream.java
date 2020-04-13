package io.ray.streaming.python.stream;

import io.ray.streaming.api.context.StreamingContext;
import io.ray.streaming.api.stream.Stream;
import io.ray.streaming.python.PythonFunction;
import io.ray.streaming.python.PythonFunction.FunctionInterface;
import io.ray.streaming.python.PythonOperator;
import io.ray.streaming.python.PythonPartition;

/**
 * Represents a stream of data whose transformations will be executed in python.
 */
public class PythonDataStream extends Stream implements PythonStream {

  protected PythonDataStream(StreamingContext streamingContext,
                             PythonOperator pythonOperator) {
    super(streamingContext, pythonOperator);
  }

  public PythonDataStream(PythonDataStream input, PythonOperator pythonOperator) {
    super(input, pythonOperator);
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
  public PythonDataStream map(PythonFunction func) {
    func.setFunctionInterface(FunctionInterface.MAP_FUNCTION);
    return new PythonDataStream(this, new PythonOperator(func));
  }

  /**
   * Apply a flat-map function to this stream.
   *
   * @param func The python FlapMapFunction.
   * @return A new PythonDataStream
   */
  public PythonDataStream flatMap(PythonFunction func) {
    func.setFunctionInterface(FunctionInterface.FLAT_MAP_FUNCTION);
    return new PythonDataStream(this, new PythonOperator(func));
  }

  /**
   * Apply a filter function to this stream.
   *
   * @param func The python FilterFunction.
   * @return A new PythonDataStream that contains only the elements satisfying
   *     the given filter predicate.
   */
  public PythonDataStream filter(PythonFunction func) {
    func.setFunctionInterface(FunctionInterface.FILTER_FUNCTION);
    return new PythonDataStream(this, new PythonOperator(func));
  }

  /**
   * Apply a sink function and get a StreamSink.
   *
   * @param func The python SinkFunction.
   * @return A new StreamSink.
   */
  public PythonStreamSink sink(PythonFunction func) {
    func.setFunctionInterface(FunctionInterface.SINK_FUNCTION);
    return new PythonStreamSink(this, new PythonOperator(func));
  }

  /**
   * Apply a key-by function to this stream.
   *
   * @param func the  python keyFunction.
   * @return A new KeyDataStream.
   */
  public PythonKeyDataStream keyBy(PythonFunction func) {
    func.setFunctionInterface(FunctionInterface.KEY_FUNCTION);
    return new PythonKeyDataStream(this, new PythonOperator(func));
  }

  /**
   * Apply broadcast to this stream.
   *
   * @return This stream.
   */
  public PythonDataStream broadcast() {
    this.partition = PythonPartition.BroadcastPartition;
    return this;
  }

  /**
   * Apply a partition to this stream.
   *
   * @param partition The partitioning strategy.
   * @return This stream.
   */
  public PythonDataStream partitionBy(PythonPartition partition) {
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
