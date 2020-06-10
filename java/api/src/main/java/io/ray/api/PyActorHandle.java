package io.ray.api;

import io.ray.api.call.PyActorCall;

/**
 * Handle of a Python actor.
 */
public interface PyActorHandle extends BaseActorHandle, PyActorCall {

  /**
   * @return Module name of the Python actor class.
   */
  String getModuleName();

  /**
   * @return Name of the Python actor class.
   */
  String getClassName();
}

