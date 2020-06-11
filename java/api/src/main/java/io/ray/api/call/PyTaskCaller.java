package io.ray.api.call;

import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.function.PyRemoteFunction;

public class PyTaskCaller<R> extends BaseTaskCaller<PyTaskCaller<R>> {
  private final PyRemoteFunction<R> func;
  private final Object[] args;

  public PyTaskCaller(PyRemoteFunction<R> func, Object[] args) {
    this.func = func;
    this.args = args;
  }

  @SuppressWarnings("unchecked")
  public ObjectRef<R> remote() {
    return Ray.internal().call(func, args, buildOptions());
  }

}
