package io.ray.streaming.python.stream;

import io.ray.streaming.api.stream.DataStream;
import io.ray.streaming.api.stream.KeyDataStream;
import io.ray.streaming.operator.ChainStrategy;
import io.ray.streaming.python.PythonFunction;
import io.ray.streaming.python.PythonFunction.FunctionInterface;
import io.ray.streaming.python.PythonOperator;
import io.ray.streaming.python.PythonPartition;

/** Represents a python DataStream returned by a key-by operation. */
@SuppressWarnings("unchecked")
public class PythonKeyDataStream extends PythonDataStream implements PythonStream {

  public PythonKeyDataStream(PythonDataStream input, PythonOperator pythonOperator) {
    super(input, pythonOperator, PythonPartition.KeyPartition);
  }

  /**
   * Create a python stream that reference passed python stream. Changes in new stream will be
   * reflected in referenced stream and vice versa
   */
  public PythonKeyDataStream(DataStream referencedStream) {
    super(referencedStream);
  }

  public PythonDataStream reduce(String moduleName, String funcName) {
    return reduce(new PythonFunction(moduleName, funcName));
  }

  /**
   * Apply a reduce function to this stream.
   *
   * @param func The reduce function.
   * @return A new DataStream.
   */
  public PythonDataStream reduce(PythonFunction func) {
    func.setFunctionInterface(FunctionInterface.REDUCE_FUNCTION);
    PythonDataStream stream = new PythonDataStream(this, new PythonOperator(func));
    stream.withChainStrategy(ChainStrategy.HEAD);
    return stream;
  }

  /**
   * Convert this stream as a java stream. The converted stream and this stream are the same logical
   * stream, which has same stream id. Changes in converted stream will be reflected in this stream
   * and vice versa.
   */
  public KeyDataStream<Object, Object> asJavaStream() {
    return new KeyDataStream(this);
  }
}
