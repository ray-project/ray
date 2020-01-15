package org.ray.api;

/**
 * Handle of a Python actor.
 */
public interface RayPyActor extends RayActor {

  /**
   * @return Module name of the Python actor class.
   */
  String getModuleName();

  /**
   * @return Name of the Python actor class.
   */
  String getClassName();
}

