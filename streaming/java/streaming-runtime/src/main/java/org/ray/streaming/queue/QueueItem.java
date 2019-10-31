package org.ray.streaming.queue;

import java.nio.ByteBuffer;

public interface QueueItem {

  /**
   * Message body maybe be a direct byte buffer, which may be invalid after call next
   * <code>QueueConsumerImpl#getBundleNative</code>. Please consume this buffer fully
   * before next call <code>getBundleNative</code>.
   *
   * @return message body
   */
  ByteBuffer body();

  /**
   * return queue item delivery time
   *
   * @return queue item delivery time
   */
  long timestamp();
}