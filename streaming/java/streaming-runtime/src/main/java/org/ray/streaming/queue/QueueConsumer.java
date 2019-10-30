package org.ray.streaming.queue;

public interface QueueConsumer {

  /**
   * pull message from input queues, if timeout, return null.
   *
   * @param timeoutMillis timeout
   * @return message or null
   */
  QueueItem pull(long timeoutMillis);

  /**
   * stop consumer to avoid blocking
   */
  void stop();

  /**
   * close queue consumer to release resource
   */
  void close();
}
