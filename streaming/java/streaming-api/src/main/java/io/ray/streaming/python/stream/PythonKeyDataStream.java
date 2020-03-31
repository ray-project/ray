package io.ray.streaming.python.stream;

import io.ray.streaming.python.PythonFunction;
import io.ray.streaming.python.PythonFunction.FunctionInterface;
import io.ray.streaming.python.PythonOperator;
import io.ray.streaming.python.PythonPartition;

/**
 * Represents a python DataStream returned by a key-by operation.
 */
public class PythonKeyDataStream extends PythonDataStream implements PythonStream  {

  public PythonKeyDataStream(PythonDataStream input, PythonOperator pythonOperator) {
    super(input, pythonOperator);
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
