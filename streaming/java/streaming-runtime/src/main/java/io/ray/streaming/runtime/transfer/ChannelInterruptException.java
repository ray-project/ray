package io.ray.streaming.runtime.transfer;

public class ChannelInterruptException extends RuntimeException {
  public ChannelInterruptException() {
    super();
  }

  public ChannelInterruptException(String message) {
    super(message);
  }
}
