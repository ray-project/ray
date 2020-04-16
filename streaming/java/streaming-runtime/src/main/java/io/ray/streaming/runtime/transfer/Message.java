package io.ray.streaming.runtime.transfer;

import java.nio.ByteBuffer;

public interface Message {

  /**
   * Message data
   *
   * Message body is a direct byte buffer, which may be invalid after call next
   * <code>DataReader#getBundleNative</code>. Please consume this buffer fully
   * before next call <code>getBundleNative</code>.
   *
   * @return message body
   */
  ByteBuffer body();

  /**
   * @return timestamp when item is written by upstream DataWriter
   */
  long timestamp();
}