package io.ray.serialization.io;

import io.ray.serialization.util.MemoryBuffer;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

public class FuryReadableByteChannel implements ReadableByteChannel {
  private boolean open = true;
  private final MemoryBuffer buffer;

  public FuryReadableByteChannel(MemoryBuffer buffer) {
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
