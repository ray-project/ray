package io.ray.api.call;

import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.function.RayFuncR;

public class TaskCaller<R> extends TaskCallerBase<TaskCaller<R>> {
  private final RayFuncR<R> func;
  private final Object[] args;

  public TaskCaller(RayFuncR<R> func, Object[] args) {
    this.func = func;
    this.args = args;
  }

  @SuppressWarnings("unchecked")
  public ObjectRef<R> remote() {
    return Ray.internal().call(func, args, createCallOptions());
  }
}
