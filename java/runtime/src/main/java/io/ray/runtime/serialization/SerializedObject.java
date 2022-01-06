package io.ray.runtime.serialization;

import io.ray.runtime.io.MemoryBuffer;
import java.nio.ByteBuffer;

/**
 * Serde serialized representation of an object. Note: This class is used for zero-copy out-of-band
 * serialization and shouldn't be used for any other cases.
 */
public interface SerializedObject {

  int totalBytes();

  /** Write serialized object to buffer. */
  void writeTo(MemoryBuffer buffer);

  /** Write serialized data as Buffer. */
  ByteBuffer toBuffer();

  class ByteBufferSerializedObject implements SerializedObject {
    private final ByteBuffer buffer;

    public ByteBufferSerializedObject(ByteBuffer buffer) {
      this.buffer = buffer;
    }

    @Override
    public int totalBytes() {
      return buffer.remaining();
    }

    @Override
    public void writeTo(MemoryBuffer buffer) {
      buffer.write(this.buffer.duplicate());
    }

    @Override
    public ByteBuffer toBuffer() {
      return buffer.duplicate();
    }
  }
}
