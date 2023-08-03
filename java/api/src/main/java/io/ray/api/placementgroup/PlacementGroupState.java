package io.ray.api.placementgroup;

/** State of placement group. */
public enum PlacementGroupState {

  /** Wait for resource to schedule. */
  PENDING(0),

  /** The placement group has created on some node. */
  CREATED(1),

  /** The placement group has removed. */
  REMOVED(2),

  /** The placement group is rescheduling. */
  RESCHEDULING(3),

  /** Unrecognized state. */
  UNRECOGNIZED(-1);

  private int value = 0;

  PlacementGroupState(int value) {
    this.value = value;
  }

  public int value() {
    return this.value;
  }
}
