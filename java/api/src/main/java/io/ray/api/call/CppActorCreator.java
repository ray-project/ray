package io.ray.api.call;

import io.ray.api.CppActorHandle;
import io.ray.api.Ray;
import io.ray.api.function.CppActorClass;

/** A helper to create cpp actor. */
public class CppActorCreator extends BaseActorCreator<CppActorCreator> {
  private final CppActorClass cppActorClass;
  private final Object[] args;

  public CppActorCreator(CppActorClass cppActorClass, Object[] args) {
    this.cppActorClass = cppActorClass;
    this.args = args;
  }

  /**
   * Create a cpp actor remotely and return a handle to the created actor.
   *
   * @return a handle to the created cpp actor.
   */
  public CppActorHandle remote() {
    return Ray.internal().createActor(cppActorClass, args, buildOptions());
  }
}
