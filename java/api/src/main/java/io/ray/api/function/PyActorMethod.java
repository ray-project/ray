package io.ray.api.function;

/**
 * A class that represents a method of a Python actor.
 * <p>
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
 * PyActorHandle actor = ...; // returned from Ray.createActor or passed from Python
 * }
 *
 * then we can call the actor method:
 *
 * {@code
 * // A.foo returns a string, so we have to set the returnType to String.class
 * ObjectRef<String> res = actor.call(PyActorMethod.of("foo", String.class));
 * String x = res.get();
 * }
 * </pre>
 */
public class PyActorMethod<R> {
  // The name of this actor method
  public final String methodName;
  // Type of the return value of this actor method
  public final Class<R> returnType;

  private PyActorMethod(String methodName, Class<R> returnType) {
    this.methodName = methodName;
    this.returnType = returnType;
  }

  /**
   * Create a python actor method.
   *
   * @param methodName The name of this actor method
   * @return a python actor method.
   */
  public static PyActorMethod<Object> of(String methodName) {
    return of(methodName, Object.class);
  }

  /**
   * Create a python actor method.
   *
   * @param methodName The name of this actor method
   * @param returnType Class of the return value of this actor method
   * @param <R> The type of the return value of this actor method
   * @return a python actor method.
   */
  public static <R> PyActorMethod<R> of(String methodName, Class<R> returnType) {
    return new PyActorMethod<>(methodName, returnType);
  }

}
