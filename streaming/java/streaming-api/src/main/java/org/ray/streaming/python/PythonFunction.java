package org.ray.streaming.python;

import org.ray.streaming.api.function.Function;

/**
 * Represents a user defined python function.
 *
 * <p>Python worker can use information in this class to create a function object.</p>
 *
 * <p>If this object is constructed from serialized python function,
 * python worker can deserialize it to create python function directly.
 * If this object is constructed from moduleName and className/functionName,
 * python worker will use `importlib` to load python function.</p>
 *
 * <p>If the python data stream api is invoked from python, `function` will be not null.</p>
 * <p>If the python data stream api is invoked from java, `moduleName` and
 * `functionName` will be not null.</p>
 * <p>
 */
public class PythonFunction implements Function {
  public enum FunctionInterface {
    SOURCE_FUNCTION("SourceFunction"),
    MAP_FUNCTION("MapFunction"),
    FLAT_MAP_FUNCTION("FlatMapFunction"),
    FILTER_FUNCTION("FilterFunction"),
    KEY_FUNCTION("KeyFunction"),
    REDUCE_FUNCTION("ReduceFunction"),
    SINK_FUNCTION("SinkFunction");

    private String functionInterface;

    /**
     * @param functionInterface function class name in `ray.streaming.function` module.
     */
    FunctionInterface(String functionInterface) {
      this.functionInterface = functionInterface;
    }
  }

  private byte[] function;
  private String moduleName;
  private String functionName;
  /**
   * FunctionInterface can be used to validate python function,
   * and look up operator class from FunctionInterface.
   */
  private String functionInterface;

  private PythonFunction(byte[] function,
                         String moduleName,
                         String functionName) {
    this.function = function;
    this.moduleName = moduleName;
    this.functionName = functionName;
  }

  public void setFunctionInterface(FunctionInterface functionInterface) {
    this.functionInterface = functionInterface.functionInterface;
  }

  public byte[] getFunction() {
    return function;
  }

  public String getModuleName() {
    return moduleName;
  }

  public String getFunctionName() {
    return functionName;
  }

  public String getFunctionInterface() {
    return functionInterface;
  }

  /**
   * Create a {@link PythonFunction} using python serialized function
   *
   * @param function serialized python function sent from python driver
   */
  public static PythonFunction fromFunction(byte[] function) {
    return new PythonFunction(function, null, null);
  }

  /**
   * Create a {@link PythonFunction} using <code>moduleName</code> and
   * <code>functionName</code>.
   *
   * @param moduleName   user function module name
   * @param functionName function/class name of the user function.
   */
  public static PythonFunction fromFunctionName(String moduleName, String functionName) {
    return new PythonFunction(null, moduleName, functionName);
  }
}
