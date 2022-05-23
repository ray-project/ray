package io.ray.runtime.io;

import static org.testng.Assert.assertTrue;

import java.nio.ByteBuffer;
import org.testng.annotations.Test;

public class BufferOutputStreamTest {
  @Test
  public void testWrite() throws Exception {
    MemoryBuffer buffer = MemoryBuffer.newHeapBuffer(32);
    MemoryBuffer buffer2 = MemoryBuffer.newHeapBuffer(32);
    try (BufferOutputStream stream = new BufferOutputStream(buffer)) {
      stream.write(ByteBuffer.allocate(100), 100);
      buffer2.write(ByteBuffer.allocate(100), 100);
      stream.write(ByteBuffer.allocateDirect(100), 100);
      buffer2.write(ByteBuffer.allocateDirect(100), 100);
      ByteBuffer byteBuffer = ByteBuffer.allocateDirect(32);
      byteBuffer.putInt(10);
      byteBuffer.putInt(10);
      byteBuffer.putInt(10);
      byteBuffer.flip();
      stream.write(byteBuffer.duplicate(), byteBuffer.remaining());
      buffer2.write(byteBuffer.duplicate());
      stream.write(1);
      stream.write(new byte[] {1, 2, 3});
      buffer2.writeByte((byte) 1);
      buffer2.writeBytes(new byte[] {1, 2, 3});
    }
    assertTrue(buffer.equalTo(buffer2, 0, 0, buffer2.writerIndex()));
  }
}
