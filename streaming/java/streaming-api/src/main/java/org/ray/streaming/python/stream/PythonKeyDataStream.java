package org.ray.streaming.python.stream;

import org.ray.streaming.api.stream.Stream;
import org.ray.streaming.operator.StreamOperator;
import org.ray.streaming.python.PythonFunction;
import org.ray.streaming.python.PythonFunction.FunctionInterface;
import org.ray.streaming.python.PythonOperator;
import org.ray.streaming.python.PythonPartition;

/**
 * Represents a python DataStream returned by a key-by operation.
 */
public class PythonKeyDataStream extends Stream implements PythonStream  {

  public PythonKeyDataStream(PythonDataStream input, StreamOperator streamOperator) {
    super(input, streamOperator);
    this.partition = PythonPartition.KeyPartition;
  }

  /**
   * Apply a reduce function to this stream.
   *
   * @param func The reduce function.
   * @return A new DataStream.
   */
  public PythonDataStream reduce(PythonFunction func) {
    func.setFunctionInterface(FunctionInterface.REDUCE_FUNCTION);
    return new PythonDataStream(this, new PythonOperator(func));
  }

  public PythonKeyDataStream setParallelism(int parallelism) {
    this.parallelism = parallelism;
    return this;
  }

}
