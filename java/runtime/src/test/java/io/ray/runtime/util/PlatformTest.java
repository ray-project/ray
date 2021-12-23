package io.ray.runtime.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class PlatformTest {

  private static final Logger LOG = LoggerFactory.getLogger(PlatformTest.class);

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
      ByteBuffer buffer2 = Platform.wrapDirectBuffer(Platform.getAddress(buffer1), 10);
      assertEquals(buffer2.getInt(), 10);
    }
    assertThrows(IllegalArgumentException.class, () -> {
      Platform.getAddress(ByteBuffer.allocate(10));
    });
  }

  @Test
  public void wrapDirectBuffer() {
    long address = 0;
    try {
      int size = 16;
      address = Platform.allocateMemory(size);
      ByteBuffer buffer = Platform.wrapDirectBuffer(address, size);
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
      long nums = 100_000_000;
      ByteBuffer buffer = null;
      {
        for (int i = 0; i < nums; i++) {
          buffer = Platform.wrapDirectBuffer(address, size);
        }
        long startTime = System.nanoTime();
        for (int i = 0; i < nums; i++) {
          buffer = Platform.wrapDirectBuffer(address, size);
        }
        long duration = System.nanoTime() - startTime;
        buffer.putLong(0, 1);
        if (LOG.isInfoEnabled()) {
          LOG.info("wrapDirectBuffer costs " + duration + "ns " + duration / 1000_000 + "ms\n");
        }
      }
      {
        for (int i = 0; i < nums; i++) {
          Platform.wrapDirectBuffer(buffer, address, size);
        }
        long startTime = System.nanoTime();
        for (int i = 0; i < nums; i++) {
          Platform.wrapDirectBuffer(buffer, address, size);
        }
        long duration = System.nanoTime() - startTime;
        buffer.putLong(0, 1);
        if (LOG.isInfoEnabled()) {
          LOG.info("wrap into buffer costs " + duration + "ns " + duration / 1000_000 + "ms\n");
        }
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
        if (LOG.isInfoEnabled()) {
          LOG.info("ByteBuffer.wrap " + duration + "ns " + duration / 1000_000 + "ms\n");
        }
      }
    } finally {
      Platform.freeMemory(address);
    }
  }

}
