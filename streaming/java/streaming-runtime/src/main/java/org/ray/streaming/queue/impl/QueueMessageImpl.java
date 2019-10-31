package org.ray.streaming.queue.impl;

import java.nio.ByteBuffer;

import org.ray.streaming.queue.QueueMessage;


public class QueueMessageImpl extends QueueItemImpl implements QueueMessage {

  private final String queueId;

  public QueueMessageImpl(String queueId, ByteBuffer data, long timestamp) {
    super(data, timestamp);
    this.queueId = queueId;
  }

  @Override
  public String queueId() {
    return queueId;
  }

}
