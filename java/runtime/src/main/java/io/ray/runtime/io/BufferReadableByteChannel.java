package io.ray.runtime.io;

import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

public class BufferReadableByteChannel implements ReadableByteChannel {
  private boolean open = true;
  private final MemoryBuffer buffer;

  public BufferReadableByteChannel(MemoryBuffer buffer) {
    this.buffer = buffer;
  }

  @Override
  public int read(ByteBuffer dst) {
    int position = dst.position();
    buffer.read(dst);
    return dst.position() - position;
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
