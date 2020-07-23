package io.ray.api.placementgroup;

/**
 * A placement group is used to place interdependent Actors according to a specific strategy.
 * When a placement group is created, the corresponding Actor slots and resources are preallocated.
 * A placement group consists of one or more bundles plus a specific placement strategy.
 */
public interface PlacementGroup {

  /**
   * Gets the bundle of the specified index.
   * @return The bundle of the specified index.
   */
  PlacementBundle getBundle(int index);
}
