package org.ray.streaming.runtime.config.types;

public enum SlotAssignStrategyType {

  /**
   * Resource scheduling strategy based on FF(First Fit) algorithm and pipeline.
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
