package io.ray.runtime.io;

import java.nio.ByteBuffer;

/** A utility class for memory-related operations. */
public class MemoryUtils {

  /**
   * Create a heap buffer of specified initial size. The buffer will grow automatically if not
   * enough.
   */
  public static MemoryBuffer buffer(int size) {
    return wrap(new byte[size]);
  }

  /**
   * Wrap a native buffer. The buffer will change into a heap buffer automatically if not enough.
   */
  public static MemoryBuffer buffer(long address, int size) {
    return new MemoryBuffer(address, size);
  }

  /** Creates a new memory buffer that targets to the given heap memory region. */
  public static MemoryBuffer wrap(byte[] buffer, int offset, int length) {
    return new MemoryBuffer(buffer, offset, length);
  }

  /** Creates a new memory buffer that targets to the given heap memory region. */
  public static MemoryBuffer wrap(byte[] buffer) {
    return new MemoryBuffer(buffer);
  }

  /**
   * Creates a new memory buffer that represents the memory backing the given byte buffer section of
   * {@code [buffer.position(), buffer,limit())}.
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
