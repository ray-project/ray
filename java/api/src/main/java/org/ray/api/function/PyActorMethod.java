package org.ray.api.function;

/**
 * A class that represents a method of a Python actor. 
 *
 * Note, information about the actor will be inferred from the actor handle, so it's not specified in this class.
 */
public class PyActorMethod {
  public String methodName;
  public Class<?> returnType;

  public PyActorMethod(String methodName, Class<?> returnType) {
    this.methodName = methodName;
    this.returnType = returnType;
  }
}
