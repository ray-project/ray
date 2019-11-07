package org.ray.streaming.runtime.queue.memory;

import java.nio.ByteBuffer;

import org.ray.streaming.runtime.queue.QueueMessage;

public class MemQueueMessageImpl implements QueueMessage {
  private final String queueId;
  private final ByteBuffer body;

  public MemQueueMessageImpl(String queueId, ByteBuffer body) {
    this.queueId = queueId;
    this.body = body;
  }

  @Override
  public String queueId() {
    return queueId;
  }

  @Override
  public ByteBuffer body() {
    return body;
  }

  @Override
  public long timestamp() {
    return System.currentTimeMillis();
  }
}