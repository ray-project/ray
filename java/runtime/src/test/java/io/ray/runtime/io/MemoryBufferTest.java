package io.ray.runtime.io;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Random;
import org.testng.annotations.Test;

public class MemoryBufferTest {

  @Test
  public void testBufferWrite() {
    MemoryBuffer buffer = MemoryBuffer.newHeapBuffer(8);
    buffer.writeBoolean(true);
    buffer.writeByte(Byte.MIN_VALUE);
    buffer.writeChar('a');
    buffer.writeShort(Short.MAX_VALUE);
    buffer.writeInt(Integer.MAX_VALUE);
    buffer.writeLong(Long.MAX_VALUE);
    buffer.writeFloat(Float.MAX_VALUE);
    buffer.writeDouble(Double.MAX_VALUE);
    byte[] bytes = new byte[] {1, 2, 3, 4};
    buffer.writeBytes(bytes);

    assertTrue(buffer.readBoolean());
    assertEquals(buffer.readByte(), Byte.MIN_VALUE);
    assertEquals(buffer.readChar(), 'a');
    assertEquals(buffer.readShort(), Short.MAX_VALUE);
    assertEquals(buffer.readInt(), Integer.MAX_VALUE);
    assertEquals(buffer.readLong(), Long.MAX_VALUE);
    assertEquals(buffer.readFloat(), Float.MAX_VALUE, 0.1);
    assertEquals(buffer.readDouble(), Double.MAX_VALUE, 0.1);
    assertEquals(buffer.readBytes(bytes.length), bytes);
    assertEquals(buffer.readerIndex(), buffer.writerIndex());
  }

  @Test
  public void testWrapBuffer() {
    {
      byte[] bytes = new byte[8];
      int offset = 2;
      bytes[offset] = 1;
      MemoryBuffer buffer = MemoryBuffer.fromByteArray(bytes, offset, 2);
      assertEquals(buffer.readByte(), bytes[offset]);
    }
    {
      byte[] bytes = new byte[8];
      int offset = 2;
      MemoryBuffer buffer = MemoryBuffer.fromByteBuffer(ByteBuffer.wrap(bytes, offset, 2));
      assertEquals(buffer.readByte(), bytes[offset]);
    }
    {
      ByteBuffer direct = ByteBuffer.allocateDirect(8);
      int offset = 2;
      direct.put(offset, (byte) 1);
      direct.position(offset);
      MemoryBuffer buffer = MemoryBuffer.fromByteBuffer(direct);
      assertEquals(buffer.readByte(), direct.get(offset));
    }
  }

  @Test
  public void testSliceAsByteBuffer() {
    byte[] data = new byte[10];
    new Random().nextBytes(data);
    {
      MemoryBuffer buffer = MemoryBuffer.fromByteArray(data, 5, 5);
      assertEquals(buffer.sliceAsByteBuffer(), ByteBuffer.wrap(data, 5, 5));
    }
    {
      ByteBuffer direct = ByteBuffer.allocateDirect(10);
      direct.put(data);
      direct.flip();
      direct.position(5);
      MemoryBuffer buffer = MemoryBuffer.fromByteBuffer(direct);
      assertEquals(buffer.sliceAsByteBuffer(), direct);
      assertEquals(
          Platform.getAddress(buffer.sliceAsByteBuffer()), Platform.getAddress(direct) + 5);
    }
    {
      long address = 0;
      try {
        address = Platform.allocateMemory(10);
        ByteBuffer direct = Platform.createDirectByteBufferFromNativeAddress(address, 10);
        direct.put(data);
        direct.flip();
        direct.position(5);
        MemoryBuffer buffer = MemoryBuffer.fromByteBuffer(direct);
        assertEquals(buffer.sliceAsByteBuffer(), direct);
        assertEquals(Platform.getAddress(buffer.sliceAsByteBuffer()), address + 5);
      } finally {
        Platform.freeMemory(address);
      }
    }
  }

  @Test
  public void testBulkOperations() {
    MemoryBuffer buffer = MemoryBuffer.newHeapBuffer(32);
    ByteBuffer buffer2 = ByteBuffer.allocate(1000);
    buffer.writeBytes(new byte[] {1, 2, 3, 4});
    buffer2.put(new byte[] {1, 2, 3, 4});
    ByteBuffer buffer1 = ByteBuffer.allocateDirect(10 * 8);
    for (int i = 0; i < 10; i++) {
      buffer1.putLong(10);
    }
    buffer1.flip();
    buffer.write(buffer1.duplicate());
    buffer2.put(buffer1.duplicate());
    buffer.write(ByteBuffer.wrap(new byte[] {1, 2, 3, 4}));
    buffer2.put(ByteBuffer.wrap(new byte[] {1, 2, 3, 4}));
    buffer2.flip();
    assertEquals(buffer.sliceAsByteBuffer(0, buffer.writerIndex()), buffer2);
    assertTrue(buffer.equalTo(MemoryBuffer.fromByteBuffer(buffer2), 0, 0, buffer.writerIndex()));
  }
}
