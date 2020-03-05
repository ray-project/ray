package org.ray.api;

import java.util.List;
import org.ray.api.id.GroupId;

/**
 * A handle to a placement group.
 */
public interface PlacementGroup {

  /**
   * @return The id of this group.
   */
  GroupId getId();

  /**
   * @return All bundles in this group.
   */
  List<Bundle> getBundles();
}
