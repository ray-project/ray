package io.ray.api.placementgroup;

/**
 * A placement group is used to place interdependent actors according to a specific strategy {@link
 * PlacementStrategy}. When a placement group is created, the corresponding actor slots and
 * resources are preallocated. A placement group consists of one or more bundles plus a specific
 * placement strategy.
 */
public interface PlacementGroup {}
