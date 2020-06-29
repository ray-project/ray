package io.ray.streaming.python;

import com.google.common.base.Preconditions;
import io.ray.streaming.api.function.Function;
import java.util.StringJoiner;
import org.apache.commons.lang3.StringUtils;

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

  // null if this function is constructed from moduleName/functionName.
  private final byte[] function;
  // null if this function is constructed from serialized python function.
  private final String moduleName;
  // null if this function is constructed from serialized python function.
  private final String functionName;
  /**
   * FunctionInterface can be used to validate python function,
   * and look up operator class from FunctionInterface.
   */
  private String functionInterface;

  /**
   * Create a {@link PythonFunction} from a serialized streaming python function.
   *
   * @param function serialized streaming python function from python driver.
   */
  public PythonFunction(byte[] function) {
    Preconditions.checkNotNull(function);
    this.function = function;
    this.moduleName = null;
    this.functionName = null;
  }

  /**
   * Create a {@link PythonFunction} from a moduleName and streaming function name.
   *
   * @param moduleName module name of streaming function.
   * @param functionName function name of streaming function. {@code functionName} is the name
   *     of a python function, or class name of subclass of `ray.streaming.function.`
   */
  public PythonFunction(String moduleName,
                        String functionName) {
    Preconditions.checkArgument(StringUtils.isNotBlank(moduleName));
    Preconditions.checkArgument(StringUtils.isNotBlank(functionName));
    this.function = null;
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

  public String toSimpleString() {
    if (function != null) {
      return "binary function";
    } else {
      return String.format("%s-%s.%s", functionInterface, moduleName, functionName);
    }
  }

  @Override
  public String toString() {
    StringJoiner stringJoiner = new StringJoiner(", ",
        PythonFunction.class.getSimpleName() + "[", "]");
    if (function != null) {
      stringJoiner.add("function=binary function");
    } else {
      stringJoiner.add("moduleName='" + moduleName + "'")
          .add("functionName='" + functionName + "'");
    }
    stringJoiner.add("functionInterface='" + functionInterface + "'");
    return stringJoiner.toString();
  }
}
