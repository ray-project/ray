package org.ray.api;

import org.ray.api.id.ActorGroupId;

/**
 * A handle to an Actor group. Now it is simply a descriptor.
 */
public interface RayActorGroup {

  /**
   * Get the ID of this Actor group.
   */
  ActorGroupId getId();
}
