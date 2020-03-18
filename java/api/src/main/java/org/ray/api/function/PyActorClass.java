package org.ray.api.function;

/**
 * A class that represents a Python actor class.
 */
public class PyActorClass {
  public final String moduleName;
  public final String className;

  public PyActorClass(String moduleName, String className) {
    this.moduleName = moduleName;
    this.className = className;
  }
}
