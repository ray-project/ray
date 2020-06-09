package io.ray.api.call;

import io.ray.api.ObjectRef;
import io.ray.api.PyActorHandle;
import io.ray.api.Ray;
import io.ray.api.function.PyActorMethod;

public class PyActorTaskCaller<R> {
  private final PyActorHandle actor;
  private final PyActorMethod<R> method;
  private final Object[] args;

  public PyActorTaskCaller(PyActorHandle actor, PyActorMethod<R> method, Object[] args) {
    this.actor = actor;
    this.method = method;
    this.args = args;
  }

  @SuppressWarnings("unchecked")
  public ObjectRef<R> remote() {
    return Ray.internal().callActor(actor, method, args);
  }

}
