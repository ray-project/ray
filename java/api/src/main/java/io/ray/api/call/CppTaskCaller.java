package io.ray.api.call;

import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.function.CppFunction;

/**
 * A helper to call cpp remote function.
 *
 * @param <R> The type of the cpp function return value
 */
public class CppTaskCaller<R> extends BaseTaskCaller<CppTaskCaller<R>> {
  private final CppFunction<R> func;
  private final Object[] args;

  public CppTaskCaller(CppFunction<R> func, Object[] args) {
    this.func = func;
    this.args = args;
  }

  /**
   * Execute a cpp function remotely and return an object reference to the result object in the
   * object store.
   *
   * @return an object reference to an object in the object store.
   */
  @SuppressWarnings("unchecked")
  public ObjectRef<R> remote() {
    return Ray.internal().call(func, args, buildOptions());
  }
}
