package org.ray.api;

/**
 * Handle of a Python actor.
 */
public interface RayPyActor<A> extends RayActor, PyActorCall<A> {

  /**
   * @return Module name of the Python actor class.
   */
  String getModuleName();

  /**
   * @return Name of the Python actor class.
   */
  String getClassName();
}

