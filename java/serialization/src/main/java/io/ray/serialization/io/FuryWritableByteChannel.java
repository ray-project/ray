package io.ray.serialization.io;

import io.ray.serialization.util.MemoryBuffer;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class FuryWritableByteChannel implements WritableByteChannel {
  private boolean open = true;
  private final MemoryBuffer buffer;

  public FuryWritableByteChannel(MemoryBuffer buffer) {
    this.buffer = buffer;
  }

  @Override
  public int write(ByteBuffer src) {
    int remaining = src.remaining();
    buffer.write(src, remaining);
    return remaining;
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public void close() {
    open = false;
  }
}
