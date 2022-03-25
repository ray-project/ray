package io.ray.runtime.io;

import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * A helper class to track the size of allocations. Writes to this stream do not copy or retain any
 * data, they just bump a size counter that can be later used to know exactly which data size needs
 * to be allocated for actual writing.
 *
 * <p>Note that {@link OutputStream} doesn't support {@link ByteBuffer}, which may incur extra copy.
 * See also {@link MockWritableByteChannel}.
 */
public class MockOutputStream extends OutputStream {
  private int totalBytes;

  // Writes the specified byte to this output stream.
  @Override
  public void write(int b) {
    totalBytes += 1;
  }

  @Override
  public void write(byte[] bytes, int offset, int length) {
    totalBytes += length;
  }

  public void write(ByteBuffer byteBuffer, int numBytes) {
    byteBuffer.position(byteBuffer.position() + numBytes);
    totalBytes += numBytes;
  }

  public int totalBytes() {
    return totalBytes;
  }
}
