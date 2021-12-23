package io.ray.runtime.util;

import java.nio.ByteBuffer;

public class MemoryUtils {

  public static MemoryBuffer buffer(int size) {
    return wrap(new byte[size]);
  }

  public static MemoryBuffer buffer(long address, int size) {
    return new MemoryBuffer(address, size);
  }

  /**
   * Creates a new memory segment that targets to the given heap memory region.
   *
   * <p>This method should be used to turn short lived byte arrays into memory segments.
   *
   * @param buffer The heap memory region.
   * @return A new memory segment that targets the given heap memory region.
   */
  public static MemoryBuffer wrap(byte[] buffer, int offset, int length) {
    return new MemoryBuffer(buffer, offset, length);
  }

  public static MemoryBuffer wrap(byte[] buffer) {
    return new MemoryBuffer(buffer);
  }

  /**
   * Creates a new memory segment that represents the memory backing the given byte buffer section
   * of [buffer.position(), buffer,limit()).
   *
   * @param buffer a direct buffer or heap buffer
   */
  public static MemoryBuffer wrap(ByteBuffer buffer) {
    if (buffer.isDirect()) {
      return new MemoryBuffer(buffer);
    } else {
      int offset = buffer.arrayOffset() + buffer.position();
      return new MemoryBuffer(buffer.array(), offset, buffer.remaining());
    }
  }
}
