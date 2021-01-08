package io.ray.api.call;

import io.ray.api.Ray;
import io.ray.api.function.RayFuncVoid;

/** A helper to call java remote function which doesn't have a return value. */
public class VoidTaskCaller extends BaseTaskCaller<VoidTaskCaller> {
  private final RayFuncVoid func;
  private final Object[] args;

  public VoidTaskCaller(RayFuncVoid func, Object[] args) {
    this.func = func;
    this.args = args;
  }

  /** Execute a function remotely. */
  public void remote() {
    Ray.internal().call(func, args, buildOptions());
  }
}
