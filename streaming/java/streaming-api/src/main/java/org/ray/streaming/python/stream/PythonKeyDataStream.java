package org.ray.streaming.python.stream;

import org.ray.streaming.api.stream.DataStream;
import org.ray.streaming.api.stream.KeyDataStream;
import org.ray.streaming.python.PythonFunction;
import org.ray.streaming.python.PythonFunction.FunctionInterface;
import org.ray.streaming.python.PythonOperator;
import org.ray.streaming.python.PythonPartition;

/**
 * Represents a python DataStream returned by a key-by operation.
 */
@SuppressWarnings("unchecked")
public class PythonKeyDataStream extends PythonDataStream implements PythonStream {

  public PythonKeyDataStream(PythonDataStream input, PythonOperator pythonOperator) {
    super(input, pythonOperator);
    super.setPartition(PythonPartition.KeyPartition);
  }

  /**
   * Create a python stream that reference passed python stream.
   * Changes in new stream will be reflected in referenced stream and vice versa
   */
  public PythonKeyDataStream(DataStream referencedStream) {
    super(referencedStream);
  }

  public PythonDataStream reduce(String moduleName, String funcName) {
    return reduce(PythonFunction.fromFunctionName(moduleName, funcName));
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

  /**
   * Convert this stream as a java stream.
   * The converted stream and this stream are the same logical stream, which has same stream id.
   * Changes in converted stream will be reflected in this stream and vice versa.
   */
  public KeyDataStream<Object, Object> asJava() {
    return new KeyDataStream(this);
  }

}
