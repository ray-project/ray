package org.ray.api;

import org.ray.api.id.GroupId;

/**
 * A handle to a life cycle group.
 */
public interface LifeCycleGroup {

  /**
   * @return The id of this group.
   */
  GroupId getId();
}
