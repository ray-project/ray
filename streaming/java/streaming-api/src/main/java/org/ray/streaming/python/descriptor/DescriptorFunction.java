package org.ray.streaming.python.descriptor;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import org.ray.streaming.api.function.Function;
import org.ray.streaming.python.MsgPackSerializer;

/**
 * A DescriptorFunction is used to describe a user defined python function.
 * Python worker can create a function object based on information in this
 * DescriptorFunction.
 *
 * <p>If DescriptorFunction is constructed from serialized python function,
 * python worker can directly deserialize to create python function.
 * If DescriptorFunction is constructed from moduleName and className/functionName,
 * python worker will use `importlib` to load python
 * function. </p>
 *
 * <p>When python data stream api call is from python, `pythonFunction` will be not null.</p>
 * <p>When python data stream api call is from java, `moduleName` and `className`/`functionName`
 * will be not null.</p>
 */
public class DescriptorFunction implements Descriptor, Function {
  public enum PythonFunctionInterface {
    SOURCE_FUNCTION("ray.streaming.function.SourceFunction"),
    MAP_FUNCTION("ray.streaming.function.MapFunction"),
    FLAT_MAP_FUNCTION("ray.streaming.function.FlatMapFunction"),
    FILTER_FUNCTION("ray.streaming.function.FilterFunction"),
    KEY_FUNCTION("ray.streaming.function.KeyFunction"),
    REDUCE_FUNCTION("ray.streaming.function.ReduceFunction"),
    SINK_FUNCTION("ray.streaming.function.SinkFunction");

    private String pythonFunctionInterface;

    PythonFunctionInterface(String pythonFunctionInterface) {
      this.pythonFunctionInterface = pythonFunctionInterface;
    }
  }

  private byte[] function;
  private String moduleName;
  private String className;
  private String functionName;
  /**
   * FunctionInterface can be used to validate python function,
   * and look up operator class from FunctionInterface.
   */
  private String functionInterface;

  private DescriptorFunction(byte[] function,
                             String moduleName,
                             String className,
                             String functionName) {
    this.function = function;
    this.moduleName = moduleName;
    this.className = className;
    this.functionName = functionName;
  }

  public void setFunctionInterface(PythonFunctionInterface functionInterface) {
    this.functionInterface = functionInterface.pythonFunctionInterface;
  }

  @Override
  public byte[] toBytes() {
    Preconditions.checkNotNull(this.functionInterface);
    return new MsgPackSerializer().serialize(
        Arrays.asList(function, moduleName, className, functionName, functionInterface));
  }

  /**
   * Create a {@link DescriptorFunction} using python serialized function
   *
   * @param pythonFunction serialized python function sent from python driver
   */
  public static DescriptorFunction fromFunction(byte[] pythonFunction) {
    return new DescriptorFunction(pythonFunction, null, null, null);
  }

  /**
   * Create a {@link DescriptorFunction} using <code>moduleName</code> and
   * <code>className</code>.
   *
   * @param moduleName python module name
   * @param className  python class name
   */
  public static DescriptorFunction fromClassName(String moduleName, String className) {
    return new DescriptorFunction(null, moduleName, className, null);
  }

  /**
   * Create a {@link DescriptorFunction} using <code>moduleName</code> and
   * <code>functionName</code>.
   *
   * @param moduleName   python module name
   * @param functionName python function name
   */
  public static DescriptorFunction fromFunctionName(String moduleName, String functionName) {
    return new DescriptorFunction(null, moduleName, null, functionName);
  }
}
