package io.ray.api.placementgroup;

/**
 * A handle to a placement group.
 */
public interface PlacementGroup {

  /**
   * Gets the bundle of the specified index.
   * @return The bundle of the specified index. If the index is invalid, return NULL.
   */
  Bundle getBundle(int index);
}