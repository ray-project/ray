package org.ray.streaming.python.descriptor;

import org.ray.streaming.api.function.Function;

/**
 * DescriptorFunction is used to describe a used defined python function.
 * <br>
 * When python data stream api call is from python, `serializedPyFunction` will be not null.
 * <br>
 * When python data stream api call is from java, `moduleName` and `className`/`functionName`
 * will be not null.
 */

/**
 * A DescriptorFunction is used to describe a used defined python function.
 * <p>
 * Python worker can create a function object based on information in this
 * DescriptorFunction.
 * <p>
 * If DescriptorFunction is constructed from python serialized function,
 * python worker can directly deserialize serialized python function to create
 * python function. If DescriptorFunction is constructed from moduleName and
 * className/functionName, python worker will use `importlib` to load python
 * function.
 */
public class DescriptorFunction implements Descriptor, Function {
  private byte[] serializedPyFunction;
  private String moduleName;
  private String className;
  private String functionName;

  /**
   * Create a {@link DescriptorFunction} using python serialized function
   *
   * @param serializedPyFunction serialized python function sent from python driver
   */
  public DescriptorFunction(byte[] serializedPyFunction) {
    this.serializedPyFunction = serializedPyFunction;
  }

  /**
   * Create a {@link DescriptorFunction} using <code>moduleName</code> and
   * <code>className</code>/<code>functionName</code>.
   *
   * @param moduleName   python module name
   * @param className    python class name
   * @param functionName python function name
   */
  public DescriptorFunction(String moduleName, String className, String functionName) {
    this.moduleName = moduleName;
    this.className = className;
    this.functionName = functionName;
  }

  @Override
  public byte[] toBytes() {
    // TODO serialize to bytes using protobuf
    return new byte[0];
  }
}
