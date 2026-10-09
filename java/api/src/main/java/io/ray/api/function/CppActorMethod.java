package io.ray.api.function;

public class CppActorMethod<R> {
  // The name of this actor method
  public final String methodName;
  // Type of the return value of this actor method
  public final Class<R> returnType;

  private CppActorMethod(String methodName, Class<R> returnType) {
    this.methodName = methodName;
    this.returnType = returnType;
  }

  /**
   * Create a cppthon actor method.
   *
   * @param methodName The name of this actor method
   * @return a cppthon actor method.
   */
  public static CppActorMethod<Object> of(String methodName) {
    return of(methodName, Object.class);
  }

  /**
   * Create a cppthon actor method.
   *
   * @param methodName The name of this actor method
   * @param returnType Class of the return value of this actor method
   * @param <R> The type of the return value of this actor method
   * @return a cppthon actor method.
   */
  public static <R> CppActorMethod<R> of(String methodName, Class<R> returnType) {
    return new CppActorMethod<>(methodName, returnType);
  }
}
