package org.ray.streaming.queue.impl;

public class QueueInterruptException extends RuntimeException {
  public QueueInterruptException() {
    super();
  }

  public QueueInterruptException(String message) {
    super(message);
  }
}
