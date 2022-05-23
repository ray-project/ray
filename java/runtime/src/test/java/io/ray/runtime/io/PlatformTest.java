package io.ray.runtime.io;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.testng.annotations.Test;

public class PlatformTest {

  @Test
  public void testArrayEquals() {
    byte[] bytes = "123456781234567".getBytes(StandardCharsets.UTF_8);
    byte[] bytes2 = "123456781234567".getBytes(StandardCharsets.UTF_8);
    assert bytes.length == bytes2.length;
    assertTrue(
        Platform.arrayEquals(
            bytes, Platform.BYTE_ARRAY_OFFSET, bytes2, Platform.BYTE_ARRAY_OFFSET, bytes.length));
  }

  @Test
  public void testGetAddress() {
    {
      ByteBuffer buffer1 = ByteBuffer.allocateDirect(10);
      buffer1.putInt(10);
      ByteBuffer buffer2 =
          Platform.createDirectByteBufferFromNativeAddress(Platform.getAddress(buffer1), 10);
      assertEquals(buffer2.getInt(), 10);
    }
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          Platform.getAddress(ByteBuffer.allocate(10));
        });
  }

  @Test
  public void wrapDirectBuffer() {
    long address = 0;
    try {
      int size = 16;
      address = Platform.allocateMemory(size);
      ByteBuffer buffer = Platform.createDirectByteBufferFromNativeAddress(address, size);
      buffer.putLong(0, 1);
      assertEquals(1, buffer.getLong(0));
    } finally {
      Platform.freeMemory(address);
    }
  }

  @Test(enabled = false)
  public void benchmarkWrapDirectBuffer() {
    long address = 0;
    try {
      int size = 16;
      address = Platform.allocateMemory(size);
      long nums = 10_000_000;
      ByteBuffer buffer = null;
      {
        for (int i = 0; i < nums; i++) {
          buffer = Platform.createDirectByteBufferFromNativeAddress(address, size);
        }
        long startTime = System.nanoTime();
        for (int i = 0; i < nums; i++) {
          buffer = Platform.createDirectByteBufferFromNativeAddress(address, size);
        }
        long duration = System.nanoTime() - startTime;
        buffer.putLong(0, 1);
        System.out.printf(
            "createDirectByteBufferFromNativeAddress costs %sns %sms\n",
            duration, duration / 1000_000);
      }
      {
        for (int i = 0; i < nums; i++) {
          buffer = ByteBuffer.allocateDirect(size);
        }
        long startTime = System.nanoTime();
        for (int i = 0; i < nums; i++) {
          buffer = ByteBuffer.allocateDirect(size);
        }
        long duration = System.nanoTime() - startTime;
        buffer.putLong(0, 1);
        System.out.printf(
            "ByteBuffer.allocateDirect costs %sns %sms\n", duration, duration / 1000_000);
      }
      {
        for (int i = 0; i < nums; i++) {
          Platform.wrapDirectByteBufferFromNativeAddress(buffer, address, size);
        }
        long startTime = System.nanoTime();
        for (int i = 0; i < nums; i++) {
          Platform.wrapDirectByteBufferFromNativeAddress(buffer, address, size);
        }
        long duration = System.nanoTime() - startTime;
        buffer.putLong(0, 1);
        System.out.printf("wrap into buffer costs %sns %sms\n", duration, duration / 1000_000);
      }
      {
        byte[] arr = new byte[32];
        ByteBuffer buf = null;
        for (int i = 0; i < nums; i++) {
          buf = ByteBuffer.wrap(arr);
        }
        long startTime = System.nanoTime();
        for (int i = 0; i < nums; i++) {
          buf = ByteBuffer.wrap(arr);
        }
        long duration = System.nanoTime() - startTime;
        buf.putLong(0, 1);
        System.out.printf("ByteBuffer.wrap %s ns %s ms\n", duration, duration / 1000_000);
      }
    } finally {
      Platform.freeMemory(address);
    }
  }
}
