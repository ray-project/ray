package org.ray.api;

import org.ray.api.id.GroupId;

/**
 * A handle to a bundle.
 */
public interface Bundle {

  /**
   * @return The id of this bundle.
   */
  GroupId getId();
}
