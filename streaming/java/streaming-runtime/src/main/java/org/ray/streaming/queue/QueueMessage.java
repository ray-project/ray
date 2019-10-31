package org.ray.streaming.queue;

public interface QueueMessage extends QueueItem {

  /**
   * return queue id
   *
   * @return queue id
   */
  String queueId();
}