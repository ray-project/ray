package io.ray.api.function;

/**
 * A class that represents a Python remote function.
 *
 * <pre>
 * example_package/
 * ├──__init__.py
 * └──example_module.py
 *
 * in example_module.py there is a function.
 *
 * \@ray.remote
 * def bar(v):
 *     return v
 *
 * then we can call the Python function bar:
 *
 * {@code
 * // bar returns input, so we have to set the returnType to int.class if bar accepts an int
 * ObjectRef<Integer> res = actor.call(
 *    PyFunction.of("example_package.example_module", "bar", Integer.class),
 *    1);
 * Integer value = res.get();
 *
 * // bar returns input, so we have to set the returnType to String.class if bar accepts a string
 * ObjectRef<String> res = actor.call(
 *    PyFunction.of("example_package.example_module", "bar", String.class),
 *    "Hello world!");
 * String value = res.get();
 * }
 * </pre>
 */
public class PyFunction<R> {
  // The full module name of this function
  public final String moduleName;
  // The name of this function
  public final String functionName;
  // Type of the return value of this function
  public final Class<R> returnType;

  private PyFunction(String moduleName, String functionName, Class<R> returnType) {
    this.moduleName = moduleName;
    this.functionName = functionName;
    this.returnType = returnType;
  }

  /**
   * Create a python function.
   *
   * @param moduleName The full module name of this function
   * @param functionName The name of this function
   * @return a python function.
   */
  public static PyFunction<Object> of(String moduleName, String functionName) {
    return of(moduleName, functionName, Object.class);
  }

  /**
   * Create a python function.
   *
   * @param moduleName The full module name of this function
   * @param functionName The name of this function
   * @param returnType Class of the return value of this function
   * @param <R> Type of the return value of this function
   * @return a python function.
   */
  public static <R> PyFunction<R> of(String moduleName, String functionName, Class<R> returnType) {
    return new PyFunction<>(moduleName, functionName, returnType);
  }
}
