package org.ray.streaming.runtime.config.types;

public enum TransferChannelType {

  /**
   * Memory type
   */
  MEMORY_CHANNEL("memory_channel", 0),

  /**
   * Native type
   */
  NATIVE_CHANNEL("native_channel", 1);

  private String value;
  private int index;

  TransferChannelType(String value, int index) {
    this.value = value;
    this.index = index;
  }

  public String getValue() {
    return value;
  }
}