package org.ray.lib.actorgroup.api.runtime;

import java.util.List;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.function.RayFunc;
import org.ray.lib.actorgroup.api.ActorGroup;
import org.ray.lib.actorgroup.api.options.ActorGroupCallOptions;

public interface RayActorGroupRuntime {

  /**
   * Invoke a remote function on an Actor Group.
   *
   * @param func The remote Actor function to run.
   * @param group A handle to the Actor group.
   * @param args The arguments of the remote function.
   * @param options The options for this call.
   * @return The result object.
   */
  RayObject call(RayFunc func, ActorGroup<?> group, Object[] args, ActorGroupCallOptions options);

  /**
   * Create a homogeneous Actor group.
   *
   * @param name The name of this Actor group.
   * @param actors Sorted Actors contained in this Actor group.
   * @param <T> The class type of these Actors.
   * @return A handle to the Actor group.
   */
  <T> ActorGroup<T> createActorGroup(String name, List<RayActor<T>> actors);

  /**
   * Get the handle of an existing Actor group by its name.
   *
   * @param name The name of this Actor group.
   * @param <T> The class type of these Actors.
   * @return The handle to the Actor group.
   */
  <T> ActorGroup<T> getActorGroup(String name);
}
