package io.ray.streaming.runtime.config.types;

public enum StateBackendType {

  /**
   * Memory type
   */
  MEMORY("memory", 0),

  /**
   * Local File
   */
  LOCAL_FILE("local_file", 1);

  private String name;
  private int index;

  StateBackendType(String name, int index) {
    this.name = name;
    this.index = index;
  }
}
