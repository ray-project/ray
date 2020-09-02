package io.ray.api.call;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.api.function.RayFuncR;

/**
 * A helper to create java actor.
 *
 * @param <A> The type of the concrete actor class.
 */
public class ActorCreator<A> extends BaseActorCreator<ActorCreator<A>> {
  private final RayFuncR<A> func;
  private final Object[] args;

  public ActorCreator(RayFuncR<A> func, Object[] args) {
    this.func = func;
    this.args = args;
  }

  /**
   * Set the JVM options for the Java worker that this actor is running in.
   *
   * Note, if this is set, this actor won't share Java worker with other actors or tasks.
   *
   * @param jvmOptions JVM options for the Java worker that this actor is running in.
   * @return self
   * @see io.ray.api.options.ActorCreationOptions.Builder#setJvmOptions(java.lang.String)
   */
  public ActorCreator<A> setJvmOptions(String jvmOptions) {
    builder.setJvmOptions(jvmOptions);
    return this;
  }

  /**
   * Create a java actor remotely and return a handle to the created actor.
   *
   * @return a handle to the created java actor.
   */
  public ActorHandle<A> remote() {
    return Ray.internal().createActor(func, args, buildOptions());
  }

}
