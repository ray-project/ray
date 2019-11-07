package org.ray.streaming.runtime.queue;

public interface QueueMessage extends QueueItem {

  /**
   * return queue id
   *
   * @return queue id
   */
  String queueId();
}