package io.ray.api.call;

import io.ray.api.ObjectRef;
import io.ray.api.PyActorHandle;
import io.ray.api.Ray;
import io.ray.api.function.PyActorMethod;

/**
 * A helper to call python actor method.
 *
 * @param <R> The type of the python actor method return value
 */
public class PyActorTaskCaller<R> {
  private final PyActorHandle actor;
  private final PyActorMethod<R> method;
  private final Object[] args;

  public PyActorTaskCaller(PyActorHandle actor, PyActorMethod<R> method, Object[] args) {
    this.actor = actor;
    this.method = method;
    this.args = args;
  }

  /**
   * Execute a python actor method remotely and return an object reference to the result object in
   * the object store.
   *
   * @return an object reference to an object in the object store.
   */
  @SuppressWarnings("unchecked")
  public ObjectRef<R> remote() {
    return Ray.internal().callActor(actor, method, args);
  }

}
