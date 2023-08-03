package io.ray.api;

/** Handle of a Cpp actor. */
public interface CppActorHandle extends BaseActorHandle, CppActorCall {

  /** Returns the name of the Cpp actor class. */
  String getClassName();
}
