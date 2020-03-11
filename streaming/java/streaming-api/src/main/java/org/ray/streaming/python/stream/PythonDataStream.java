package org.ray.streaming.python.stream;

import org.ray.streaming.api.Language;
import org.ray.streaming.api.context.StreamingContext;
import org.ray.streaming.api.partition.Partition;
import org.ray.streaming.api.stream.DataStream;
import org.ray.streaming.api.stream.Stream;
import org.ray.streaming.python.PythonFunction;
import org.ray.streaming.python.PythonFunction.FunctionInterface;
import org.ray.streaming.python.PythonOperator;
import org.ray.streaming.python.PythonPartition;

/**
 * Represents a stream of data whose transformations will be executed in python.
 */
public class PythonDataStream extends Stream<PythonDataStream, Object> implements PythonStream {

  protected PythonDataStream(StreamingContext streamingContext,
                             PythonOperator pythonOperator) {
    super(streamingContext, pythonOperator);
  }

  protected PythonDataStream(StreamingContext streamingContext,
                             PythonOperator pythonOperator,
                             Partition<Object> partition) {
    super(streamingContext, pythonOperator, partition);
  }

  public PythonDataStream(PythonDataStream input, PythonOperator pythonOperator) {
    super(input, pythonOperator);
  }

  public PythonDataStream(PythonDataStream input,
                          PythonOperator pythonOperator,
                          Partition<Object> partition) {
    super(input, pythonOperator, partition);
  }

  /**
   * Create a python stream that reference passed java stream.
   * Changes in new stream will be reflected in referenced stream and vice versa
   */
  public PythonDataStream(DataStream referencedStream) {
    super(referencedStream);
  }

  public PythonDataStream map(String moduleName, String funcName) {
    return map(new PythonFunction(moduleName, funcName));
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

  public PythonDataStream flatMap(String moduleName, String funcName) {
    return flatMap(new PythonFunction(moduleName, funcName));
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

  public PythonDataStream filter(String moduleName, String funcName) {
    return filter(new PythonFunction(moduleName, funcName));
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

  public PythonStreamSink sink(String moduleName, String funcName) {
    return sink(new PythonFunction(moduleName, funcName));
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

  public PythonKeyDataStream keyBy(String moduleName, String funcName) {
    return keyBy(new PythonFunction(moduleName, funcName));
  }

  /**
   * Apply a key-by function to this stream.
   *
   * @param func the  python keyFunction.
   * @return A new KeyDataStream.
   */
  public PythonKeyDataStream keyBy(PythonFunction func) {
    checkPartitionCall();
    func.setFunctionInterface(FunctionInterface.KEY_FUNCTION);
    return new PythonKeyDataStream(this, new PythonOperator(func));
  }

  /**
   * Apply broadcast to this stream.
   *
   * @return This stream.
   */
  public PythonDataStream broadcast() {
    checkPartitionCall();
    return setPartition(PythonPartition.BroadcastPartition);
  }

  /**
   * Apply a partition to this stream.
   *
   * @param partition The partitioning strategy.
   * @return This stream.
   */
  public PythonDataStream partitionBy(PythonPartition partition) {
    checkPartitionCall();
    return setPartition(partition);
  }

  /**
   * If parent stream is a python stream, we can't call partition related methods
   * in the java stream.
   */
  private void checkPartitionCall() {
    if (getInputStream() != null && getInputStream().getLanguage() == Language.JAVA) {
      throw new RuntimeException("Partition related methods can't be called on a " +
          "python stream if parent stream is a java stream.");
    }
  }

  /**
   * Convert this stream as a java stream.
   * The converted stream and this stream are the same logical stream, which has same stream id.
   * Changes in converted stream will be reflected in this stream and vice versa.
   */
  public DataStream<Object> asJava() {
    return new DataStream<>(this);
  }

  @Override
  public Language getLanguage() {
    return Language.PYTHON;
  }

}
