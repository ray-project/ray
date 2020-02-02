package org.ray.api;

import org.ray.api.id.GroupId;

/**
 * A handle to a slot set.
 */
public interface SlotSet {

  /**
   * @return The id of this slot set.
   */
  GroupId getId();
}
