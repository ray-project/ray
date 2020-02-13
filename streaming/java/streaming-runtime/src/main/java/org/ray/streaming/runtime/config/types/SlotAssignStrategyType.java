package org.ray.streaming.runtime.config.types;

/**
 * Slot assign strategy type.
 */
public enum SlotAssignStrategyType {

  /**
   * PIPELINE_FIRST_STRATEGY
   */
  PIPELINE_FIRST_STRATEGY("pipeline_first_strategy", 0);

  private String value;
  private int index;

  SlotAssignStrategyType(String value, int index) {
    this.value = value;
    this.index = index;
  }

  public String getValue() {
    return value;
  }
}
