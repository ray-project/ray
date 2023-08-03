package io.ray.api;

/** Handle of a Python actor. */
public interface PyActorHandle extends BaseActorHandle, PyActorCall {

  /** Returns the module name of the Python actor class. */
  String getModuleName();

  /** Returns the name of the Python actor class. */
  String getClassName();
}
