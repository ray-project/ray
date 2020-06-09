package io.ray.api.call;

import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.function.RayFunc;

public class TaskCaller<R> extends TaskCallerBase {
  private final RayFunc<R> func;
  private final Object[] args;

  public TaskCaller(RayFunc<R> func, Object[] args) {
    this.func = func;
    this.args = args;
  }

  @SuppressWarnings("unchecked")
  public ObjectRef<R> remote() {
    return Ray.internal().call(func, args, createCallOptions());
  }
}
