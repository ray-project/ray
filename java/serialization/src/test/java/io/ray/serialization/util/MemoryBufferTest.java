package io.ray.serialization.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MemoryBufferTest {

  @Test
  public void testBufferWrite() {
    MemoryBuffer buffer = MemoryUtils.buffer(8);
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
      MemoryBuffer buffer = MemoryUtils.wrap(bytes, offset, 2);
      assertEquals(buffer.readByte(), bytes[offset]);
    }
    {
      byte[] bytes = new byte[8];
      int offset = 2;
      MemoryBuffer buffer = MemoryUtils.wrap(ByteBuffer.wrap(bytes, offset, 2));
      assertEquals(buffer.readByte(), bytes[offset]);
    }
    {
      ByteBuffer direct = ByteBuffer.allocateDirect(8);
      int offset = 2;
      direct.put(offset, (byte) 1);
      direct.position(offset);
      MemoryBuffer buffer = MemoryUtils.wrap(direct);
      assertEquals(buffer.readByte(), direct.get(offset));
    }
  }

  @Test
  public void testSliceAsByteBuffer() {
    byte[] data = new byte[10];
    new Random().nextBytes(data);
    {
      MemoryBuffer buffer = MemoryUtils.wrap(data, 5, 5);
      assertEquals(buffer.sliceAsByteBuffer(), ByteBuffer.wrap(data, 5, 5));
    }
    {
      ByteBuffer direct = ByteBuffer.allocateDirect(10);
      direct.put(data);
      direct.flip();
      direct.position(5);
      MemoryBuffer buffer = MemoryUtils.wrap(direct);
      assertEquals(buffer.sliceAsByteBuffer(), direct);
      assertEquals(
          Platform.getAddress(buffer.sliceAsByteBuffer()), Platform.getAddress(direct) + 5);
    }
    {
      long address = 0;
      try {
        address = Platform.allocateMemory(10);
        ByteBuffer direct = Platform.wrapDirectBuffer(address, 10);
        direct.put(data);
        direct.flip();
        direct.position(5);
        MemoryBuffer buffer = MemoryUtils.wrap(direct);
        assertEquals(buffer.sliceAsByteBuffer(), direct);
        assertEquals(Platform.getAddress(buffer.sliceAsByteBuffer()), address + 5);
      } finally {
        Platform.freeMemory(address);
      }
    }
  }

  @Test
  public void testCompare() {
    MemoryBuffer buf1 = MemoryUtils.buffer(16);
    MemoryBuffer buf2 = MemoryUtils.buffer(16);
    buf1.putLongB(0, 10);
    buf2.putLongB(0, 10);
    buf1.put(9, (byte) 1);
    buf2.put(9, (byte) 2);
    Assert.assertTrue(buf1.compare(buf2, 0, 0, buf1.size()) < 0);
    buf1.put(9, (byte) 3);
    Assert.assertFalse(buf1.compare(buf2, 0, 0, buf1.size()) < 0);
  }

  @Test
  public void testEqualTo() {
    MemoryBuffer buf1 = MemoryUtils.buffer(16);
    MemoryBuffer buf2 = MemoryUtils.buffer(16);
    buf1.putLongB(0, 10);
    buf2.putLongB(0, 10);
    buf1.put(9, (byte) 1);
    buf2.put(9, (byte) 1);
    Assert.assertTrue(buf1.equalTo(buf2, 0, 0, buf1.size()));
    buf1.put(9, (byte) 2);
    Assert.assertFalse(buf1.equalTo(buf2, 0, 0, buf1.size()));
  }
}
