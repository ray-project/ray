package io.ray.api.function;

public class CppActorClass {
  // The name of function creating the class.
  public final String createFunctionName;
  // The name of this actor class
  public final String className;

  private CppActorClass(String createFunctionName, String className) {
    this.createFunctionName = createFunctionName;
    this.className = className;
  }

  /**
   * Create a cpp actor class.
   *
   * @param createFunctionName The name of function creating the class
   * @param className The name of this actor class
   * @return a cpp actor class
   */
  public static CppActorClass of(String createFunctionName, String className) {
    return new CppActorClass(createFunctionName, className);
  }
}
