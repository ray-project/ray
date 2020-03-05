package org.ray.api.options;

public enum PlacementStrategy {

  /**
   * Packs Bundles close together inside processes or nodes as tight as possible.
   */
  PACK,

  /**
   * Places Bundles across distinct nodes or processes as even as possible.
   */
  SPREAD,
}
