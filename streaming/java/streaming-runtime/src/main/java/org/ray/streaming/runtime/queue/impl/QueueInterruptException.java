package org.ray.streaming.runtime.queue.impl;

public class QueueInterruptException extends RuntimeException {
  public QueueInterruptException() {
    super();
  }

  public QueueInterruptException(String message) {
    super(message);
  }
}
