package io.ray.api.call;

import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.function.PyFunction;

/**
 * A helper to call python remote function.
 *
 * @param <R> The type of the python function return value
 */
public class PyTaskCaller<R> extends BaseTaskCaller<PyTaskCaller<R>> {
  private final PyFunction<R> func;
  private final Object[] args;

  public PyTaskCaller(PyFunction<R> func, Object[] args) {
    this.func = func;
    this.args = args;
  }

  /**
   * Execute a python function remotely and return an object reference to the result object in the
   * object store.
   *
   * <p>Returns an object reference to an object in the object store.
   */
  @SuppressWarnings("unchecked")
  public ObjectRef<R> remote() {
    return Ray.internal().call(func, args, buildOptions());
  }
}
