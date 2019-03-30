package org.ray.runtime.functionmanager;

/**
 * Represents metadata of a Python function.
 */
public class PyFunctionDescriptor implements FunctionDescriptor {

  public String moduleName;

  public String className;

  public String functionName;

  public PyFunctionDescriptor(String moduleName, String className, String functionName) {
    this.moduleName = moduleName;
    this.className = className;
    this.functionName = functionName;
  }

  @Override
  public String toString() {
    return moduleName + "." + className + "." + functionName;
  }
}

