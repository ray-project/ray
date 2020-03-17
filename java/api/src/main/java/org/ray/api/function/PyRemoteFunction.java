package org.ray.api.function;

/**
 * A class for calling Python remote function
 */
public class PyRemoteFunction {
  public String moduleName;
  public String functionName;
  public Class<?> returnType;

  public PyRemoteFunction(String moduleName, String functionName, Class<?> returnType) {
    this.moduleName = moduleName;
    this.functionName = functionName;
    this.returnType = returnType;
  }
}
