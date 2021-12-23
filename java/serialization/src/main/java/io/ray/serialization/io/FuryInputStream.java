package io.ray.serialization.io;

import io.ray.serialization.util.MemoryBuffer;
import java.io.IOException;
import java.io.InputStream;

public class FuryInputStream extends InputStream {
  private final MemoryBuffer buffer;

  public FuryInputStream(MemoryBuffer buffer) {
    this.buffer = buffer;
  }

  public int read() {
    if (buffer.remaining() == 0) {
      return -1;
    } else {
      return buffer.readByte() & 0xFF;
    }
  }

  public int read(byte[] bytes, int offset, int length) throws IOException {
    if (length == 0) {
      return 0;
    }
    int size = Math.min(buffer.remaining(), length);
    if (size == 0) {
      return -1;
    }
    buffer.readBytes(bytes, offset, size);
    return size;
  }

  public int available() throws IOException {
    return buffer.remaining();
  }
}
