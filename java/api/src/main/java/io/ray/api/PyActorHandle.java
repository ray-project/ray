package io.ray.api;

/** Handle of a Python actor. */
public interface PyActorHandle extends BaseActorHandle, PyActorCall {

  /** @return the module name of the Python actor class. */
  String getModuleName();

  /** @return the name of the Python actor class. */
  String getClassName();
}
