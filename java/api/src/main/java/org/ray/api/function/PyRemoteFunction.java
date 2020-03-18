package org.ray.api.function;

/**
 * A class for calling Python remote function
 */
public class PyRemoteFunction<R> {
  public final String moduleName;
  public final String functionName;
  public final Class<R> returnType;

  public PyRemoteFunction(String moduleName, String functionName, Class<R> returnType) {
    this.moduleName = moduleName;
    this.functionName = functionName;
    this.returnType = returnType;
  }
}
