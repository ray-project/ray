package io.ray.api.function;

/**
 * A class that represents a method of a Python actor. 
 *
 * Note, information about the actor will be inferred from the actor handle,
 * so it's not specified in this class.
 *
 * <pre>
 * there is a Python actor class A.
 *
 * \@ray.remote
 * class A(object):
 *     def foo(self):
 *         return "Hello world!"
 *
 * suppose we have got the Python actor class A's handle in Java
 *
 * {@code
 * RayPyActor actor = ...; // returned from Ray.createActor or passed from Python
 * }
 *
 * then we can call the actor method:
 *
 * {@code
 * // A.foo returns a string, so we have to set the returnType to String.class
 * RayObject<String> res = actor.call(new PyActorMethod<>("foo", String.class));
 * String x = res.get();
 * }
 * </pre>
 */
public class PyActorMethod<R> {
  // The name of this actor method
  public final String methodName;
  // Type of the return value of this actor method
  public final Class<R> returnType;

  public PyActorMethod(String methodName, Class<R> returnType) {
    this.methodName = methodName;
    this.returnType = returnType;
  }
}
