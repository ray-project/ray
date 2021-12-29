package io.ray.runtime.io;

import static org.testng.Assert.assertTrue;

import java.nio.ByteBuffer;
import org.testng.annotations.Test;

public class BufferWritableByteChannelTest {
  @Test
  public void testWrite() {
    MemoryBuffer buffer = MemoryBuffer.newHeapBuffer(32);
    MemoryBuffer buffer2 = MemoryBuffer.newHeapBuffer(32);
    try (BufferWritableByteChannel channel = new BufferWritableByteChannel(buffer)) {
      channel.write(ByteBuffer.allocate(100));
      buffer2.write(ByteBuffer.allocate(100));
      channel.write(ByteBuffer.allocateDirect(100));
      buffer2.write(ByteBuffer.allocateDirect(100));
      ByteBuffer byteBuffer = ByteBuffer.allocateDirect(32);
      byteBuffer.putInt(10);
      byteBuffer.putInt(10);
      byteBuffer.putInt(10);
      byteBuffer.flip();
      channel.write(byteBuffer.duplicate());
      buffer2.write(byteBuffer.duplicate());
    }
    assertTrue(buffer.equalTo(buffer2, 0, 0, buffer2.writerIndex()));
  }
}
