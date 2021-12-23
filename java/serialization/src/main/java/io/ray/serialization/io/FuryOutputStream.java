package io.ray.serialization.io;

import io.ray.serialization.util.MemoryBuffer;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class FuryOutputStream extends OutputStream {
  private final MemoryBuffer buffer;

  public FuryOutputStream(MemoryBuffer buffer) {
    this.buffer = buffer;
  }

  public void write(int b) {
    buffer.writeByte((byte) b);
  }

  public void write(byte[] bytes, int offset, int length) {
    buffer.writeBytes(bytes, offset, length);
  }

  public void write(ByteBuffer byteBuffer, int numBytes) {
    buffer.write(byteBuffer, numBytes);
  }
}
