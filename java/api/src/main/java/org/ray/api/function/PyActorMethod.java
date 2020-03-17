package org.ray.api.function;

/**
 * A class for calling actor method.
 */
public class PyActorMethod {
  public String methodName;
  public Class<?> returnType;

  public PyActorMethod(String methodName, Class<?> returnType) {
    this.methodName = methodName;
    this.returnType = returnType;
  }
}
