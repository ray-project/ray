package io.ray.api.function;

/**
 * A class that represents a Python actor class.
 *
 * <pre>
 * example_package/
 * ├──__init__.py
 * └──example_module.py
 *
 * in example_module.py there is an actor class A.
 *
 * \@ray.remote
 * class A(object):
 *     def __init__(self, x):
 *         self.x = x
 *
 * we can create this Python actor from Java:
 *
 * {@code
 * PyActorHandle actor = Ray.createActor(PyActorClass.of("example_package.example_module", "A"),
 *                                    "the value for x");
 * }
 * </pre>
 */
public class PyActorClass {
  // The full module name of this actor class
  public final String moduleName;
  // The name of this actor class
  public final String className;

  private PyActorClass(String moduleName, String className) {
    this.moduleName = moduleName;
    this.className = className;
  }

  /**
   * Create a python actor class.
   *
   * @param moduleName The full module name of this actor class
   * @param className The name of this actor class
   * @return a python actor class
   */
  public static PyActorClass of(String moduleName, String className) {
    return new PyActorClass(moduleName, className);
  }

}
