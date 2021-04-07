package io.ray.streaming.runtime.config.types;

public enum ContextBackendType {

  /** Memory type */
  MEMORY("memory", 0),

  /** Local File */
  LOCAL_FILE("local_file", 1);

  private String name;
  private int index;

  ContextBackendType(String name, int index) {
    this.name = name;
    this.index = index;
  }
}
