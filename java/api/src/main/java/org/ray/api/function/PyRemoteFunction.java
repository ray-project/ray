package org.ray.api.function;

/**
 * A class that represents a Python remote function.
 *
 * example_package/
 * ├──__init__.py
 * └──example_module.py
 *
 * in example_module.py there is a function.
 *
 * @ray.remote
 * def bar(v):
 *     return v
 *
 * then we can call the Python function bar:
 *
 * // bar returns input, so we have to set the returnType to int.class if bar accepts an int
 * RayObject<int> res = actor.call(
 *    new PyRemoteFunction<>("example_package.example_module", "bar", int.class),
 *    1);
 * int value = res.get();
 *
 * // bar returns input, so we have to set the returnType to String.class if bar accepts a String
 * RayObject<String> res = actor.call(
 *    new PyRemoteFunction<>("example_package.example_module", "bar", String.class),
 *    "Hello world!");
 * String value = res.get();
 */
public class PyRemoteFunction<R> {
  // The full module name of this function
  public final String moduleName;
  // The name of this function
  public final String functionName;
  // Type of the return value of this function
  public final Class<R> returnType;

  public PyRemoteFunction(String moduleName, String functionName, Class<R> returnType) {
    this.moduleName = moduleName;
    this.functionName = functionName;
    this.returnType = returnType;
  }
}
