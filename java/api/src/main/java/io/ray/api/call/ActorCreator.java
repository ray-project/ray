package io.ray.api.call;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.api.concurrencygroup.ConcurrencyGroup;
import io.ray.api.function.RayFuncR;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
    /// Handle statically defined concurrency groups.
    builder.setConcurrencyGroups(Ray.internal().extractConcurrencyGroups(this.func));
  }

  /**
   * Set the JVM options for the Java worker that this actor is running in.
   *
   * <p>Note, if this is set, this actor won't share Java worker with other actors or tasks.
   *
   * @param jvmOptions JVM options for the Java worker that this actor is running in.
   * @return self
   * @see io.ray.api.options.ActorCreationOptions.Builder#setJvmOptions(List)
   */
  public ActorCreator<A> setJvmOptions(List<String> jvmOptions) {
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

  /** Set the concurrency groups for this actor to declare how to perform tasks concurrently. */
  public ActorCreator<A> setConcurrencyGroups(ConcurrencyGroup... groups) {
    ArrayList<ConcurrencyGroup> list = new ArrayList<>();
    Collections.addAll(list, groups);
    builder.setConcurrencyGroups(list);
    return this;
  }
}
