package io.ray.runtime.io;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class BufferWritableByteChannel implements WritableByteChannel {
  private boolean open = true;
  private final MemoryBuffer buffer;

  public BufferWritableByteChannel(MemoryBuffer buffer) {
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
