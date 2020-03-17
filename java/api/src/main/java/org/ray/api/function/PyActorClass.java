package org.ray.api.function;

/**
 * A class for creating Python actor.
 */
public class PyActorClass {
  public String moduleName;
  public String className;

  public PyActorClass(String moduleName, String className) {
    this.moduleName = moduleName;
    this.className = className;
  }
}
