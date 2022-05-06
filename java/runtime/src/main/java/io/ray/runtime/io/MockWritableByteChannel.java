package io.ray.runtime.io;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * A helper class to track the size of allocations. Writes to this stream do not copy or retain any
 * data, they just bump a size counter that can be later used to know exactly which data size needs
 * to be allocated for actual writing.
 */
public class MockWritableByteChannel implements WritableByteChannel {
  private boolean open = true;
  private int totalBytes;

  @Override
  public int write(ByteBuffer src) {
    int remaining = src.remaining();
    src.position(src.limit());
    totalBytes += remaining;
    return remaining;
  }

  public int totalBytes() {
    return totalBytes;
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
