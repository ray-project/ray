package org.ray.streaming.runtime.queue.impl;

import java.nio.ByteBuffer;

import org.ray.streaming.runtime.queue.QueueItem;

public class QueueItemImpl implements QueueItem {

  private final ByteBuffer buffer;
  private final long timestamp;

  public QueueItemImpl(ByteBuffer buffer, long timestamp) {
    this.buffer = buffer;
    this.timestamp = timestamp;
  }

  @Override
  public ByteBuffer body() {
    return buffer;
  }

  @Override
  public String toString() {
    return "QueueItem: " + buffer.remaining() + "bytes.";
  }

  @Override
  public long timestamp() {
    return timestamp;
  }
}
