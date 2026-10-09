package io.ray.api.call;

import io.ray.api.CppActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.function.CppActorMethod;

/**
 * A helper to call cppthon actor method.
 *
 * @param <R> The type of the cpp actor method return value
 */
public class CppActorTaskCaller<R> {
  private final CppActorHandle actor;
  private final CppActorMethod<R> method;
  private final Object[] args;

  public CppActorTaskCaller(CppActorHandle actor, CppActorMethod<R> method, Object[] args) {
    this.actor = actor;
    this.method = method;
    this.args = args;
  }

  /**
   * Execute a cpp actor method remotely and return an object reference to the result object in the
   * object store.
   *
   * @return an object reference to an object in the object store.
   */
  @SuppressWarnings("unchecked")
  public ObjectRef<R> remote() {
    return Ray.internal().callActor(actor, method, args);
  }
}
