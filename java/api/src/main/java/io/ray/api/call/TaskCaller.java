package io.ray.api.call;

import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.function.RayFuncR;

/**
 * A helper to call java remote function.
 *
 * @param <R> The type of the java remote function return value
 */
public class TaskCaller<R> extends BaseTaskCaller<TaskCaller<R>> {
  private final RayFuncR<R> func;
  private final Object[] args;

  public TaskCaller(RayFuncR<R> func, Object[] args) {
    this.func = func;
    this.args = args;
  }

  /**
   * Execute a java function remotely and return an object reference to the result object in the
   * object store.
   *
   * <p>Returns an object reference to an object in the object store.
   */
  @SuppressWarnings("unchecked")
  public ObjectRef<R> remote() {
    return Ray.internal().call(func, args, buildOptions());
  }
}
