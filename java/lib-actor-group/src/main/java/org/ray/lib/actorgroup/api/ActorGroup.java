package org.ray.lib.actorgroup.api;

import java.util.List;
import org.ray.api.RayActor;
import org.ray.lib.actorgroup.api.id.ActorGroupId;

/**
 * A handle to an Actor group.
 *
 * @param <T> The type of the concrete Actor class in the group.
 */
public interface ActorGroup<T> {

  /**
   * Get the id of this Actor group.
   */
  ActorGroupId getId();

  /**
   * Get the name of this Actor group.
   */
  String getName();

  /**
   * Get the sorted Actors belonging to this Actor group.
   */
  List<RayActor<T>> getActors();
}
