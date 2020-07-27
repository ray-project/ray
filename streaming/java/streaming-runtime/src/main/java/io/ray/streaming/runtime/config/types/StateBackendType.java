package io.ray.streaming.runtime.config.types;

public enum StateBackendType {

  /**
   * Memory type
   */
  MEMORY("memory", 0),

  /**
   * Pangu type
   */
  PANGU("pangu", 1),

  /**
   * HBase type
   */
  HBASE("hbase", 2);

  private String name;
  private int index;

  StateBackendType(String name, int index) {
    this.name = name;
    this.index = index;
  }
}
