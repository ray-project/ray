package org.ray.api.function;

/**
 * A class that represents a method of a Python actor. 
 *
 * Note, information about the actor will be inferred from the actor handle,
 * so it's not specified in this class.
 */
public class PyActorMethod<R> {
  public final String methodName;
  public final Class<R> returnType;

  public PyActorMethod(String methodName, Class<R> returnType) {
    this.methodName = methodName;
    this.returnType = returnType;
  }
}
