package io.ray.api.placementgroup;

/**
 * State of Placement Group.
 */
public enum PlacementGroupState {

  /**
   * Wait for resource to schedule.
   */
  PENDING(0),

  /**
   * The Placement Group has created on some node.
   */
  CREATED(1),

  /**
   * The Placement Group has removed.
   */
  REMOVED(2),

  /**
   * The Placement Group are rescheduling.
   */
  RESCHEDULING(3),

  /**
   * Unrecognized State.
   */
  UNRECOGNIZED(-1);

  private int value = 0;

  PlacementGroupState(int value) {
    this.value = value;
  }

  public int value() {
    return this.value;
  }
}
