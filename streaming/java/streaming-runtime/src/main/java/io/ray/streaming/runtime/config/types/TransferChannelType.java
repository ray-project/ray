package io.ray.streaming.runtime.config.types;

/**
 * Data transfer channel type.
 */
public enum TransferChannelType {

  /**
   * Memory queue.
   */
  MEMORY_CHANNEL("memory_channel", 0),

  /**
   * Native queue.
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
