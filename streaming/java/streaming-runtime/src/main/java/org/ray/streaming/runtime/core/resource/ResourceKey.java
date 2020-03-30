package org.ray.streaming.runtime.core.resource;

/**
 * Key for different type of resources.
 */
public enum ResourceKey {

  /**
   *Cpu resource key.
   */
  CPU("CPU"),

  /**
   *Gpu resource key.
   */
  GPU("GPU"),

  /**
   * Memory resource key.
   */
  MEM("MEM");

  private String value;

  ResourceKey(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }

}
