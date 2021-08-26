package io.ray.streaming.runtime.transfer.exception;

import io.ray.streaming.runtime.transfer.DataReader;
import io.ray.streaming.runtime.transfer.DataWriter;
import io.ray.streaming.runtime.transfer.channel.ChannelId;
import java.nio.ByteBuffer;

/**
 * when {@link DataReader#stop()} or {@link DataWriter#stop()} is called, this exception might be
 * thrown in {@link DataReader#read(long)} and {@link DataWriter#write(ChannelId, ByteBuffer)},
 * which means the read/write operation is failed.
 */
public class ChannelInterruptException extends RuntimeException {

  public ChannelInterruptException() {
    super();
  }

  public ChannelInterruptException(String message) {
    super(message);
  }
}
