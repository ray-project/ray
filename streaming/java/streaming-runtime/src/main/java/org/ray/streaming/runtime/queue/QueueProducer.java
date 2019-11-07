package org.ray.streaming.runtime.queue;

import java.nio.ByteBuffer;
import java.util.Set;

public interface QueueProducer {
  /**
   * produce msg into the specified queue
   *
   * @param id   queue id
   * @param item message item data section is specified by [position, limit).
   */
  void produce(QueueID id, ByteBuffer item);

  /**
   * produce msg into the specified queues
   *
   * @param id   queue ids
   * @param item message item data section is specified by [position, limit).
   *            item doesn't have to be a direct buffer.
   */
  void produce(Set<QueueID> id, ByteBuffer item);

  /**
   * stop produce to avoid blocking
   */
  void stop();

  /**
   * close produce to release resource
   */
  void close();

}
