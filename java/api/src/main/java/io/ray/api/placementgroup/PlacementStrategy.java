package io.ray.api.placementgroup;

/** The actor placement strategy. */
public enum PlacementStrategy {
  /** Packs Bundles close together inside nodes as tight as possible. */
  PACK(0),

  /** Places Bundles across distinct nodes as even as possible. */
  SPREAD(1),

  /** Packs Bundles into one node. The group is not allowed to span multiple nodes. */
  STRICT_PACK(2),

  /**
   * Places Bundles across distinct nodes. The group is not allowed to deploy more than one bundle
   * on a node.
   */
  STRICT_SPREAD(3),

  /** Unrecognized strategy. */
  UNRECOGNIZED(-1);

  private int value = 0;

  PlacementStrategy(int value) {
    this.value = value;
  }

  public int value() {
    return this.value;
  }
}
