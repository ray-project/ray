package io.ray.streaming.runtime.config.types;

public enum ResourceAssignStrategyType {

  /**
   * Resource scheduling strategy based on FF(First Fit) algorithm and pipeline.
   */
  PIPELINE_FIRST_STRATEGY("pipeline_first_strategy", 0);

  private String name;
  private int index;

  ResourceAssignStrategyType(String name, int index) {
    this.name = name;
    this.index = index;
  }

  public String getName() {
    return name;
  }
}
