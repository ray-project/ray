package io.ray.api.function;

public class CppFunction<R> {
  // The name of this function
  public final String functionName;
  // Type of the return value of this function
  public final Class<R> returnType;

  private CppFunction(String functionName, Class<R> returnType) {
    this.functionName = functionName;
    this.returnType = returnType;
  }

  /**
   * Create a cpp function.
   *
   * @param functionName The name of this function
   * @return a cpp function.
   */
  public static CppFunction<Object> of(String functionName) {
    return of(functionName, Object.class);
  }

  /**
   * Create a cpp function.
   *
   * @param functionName The name of this function
   * @param returnType Class of the return value of this function
   * @param <R> Type of the return value of this function
   * @return a cpp function.
   */
  public static <R> CppFunction<R> of(String functionName, Class<R> returnType) {
    return new CppFunction<>(functionName, returnType);
  }
}
