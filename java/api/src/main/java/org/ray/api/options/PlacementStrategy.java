package org.ray.api.options;

public enum PlacementStrategy {

  /**
   * Packs Actors close together inside processes or nodes as tight as possible.
   */
  BUNDLE,

  /**
   * Try to bundle Actors in the same slot set while spreading slot sets over nodes.
   */
  PARTITION,

  /**
   * Try to bundle Actors with the same serial number in their own slot sets while spreading
   * different serial numbers over nodes.
   */
  PIPELINE,

  /**
   * Places Actors across distinct nodes or processes as even as possible.
   */
  SPREAD,
}
