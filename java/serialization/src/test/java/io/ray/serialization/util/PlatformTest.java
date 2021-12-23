package io.ray.serialization.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.slf4j.Logger;
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

  @Test(enabled = false)
  public void benchmarkArrayEquals() {
    byte[] bytes = "123456781234567".getBytes(StandardCharsets.UTF_8);
    byte[] bytes2 = "123456781234567".getBytes(StandardCharsets.UTF_8);
    arrayEquals(bytes, bytes2);
    bytes = "1234567812345678".getBytes(StandardCharsets.UTF_8);
    bytes2 = "1234567812345678".getBytes(StandardCharsets.UTF_8);
    arrayEquals(bytes, bytes2);
  }

  private boolean arrayEquals(byte[] bytes, byte[] bytes2) {
    long nums = 200_000_000;
    boolean eq = false;
    {
      // warm
      for (int i = 0; i < nums; i++) {
        eq =
            bytes.length == bytes2.length
                && Platform.arrayEquals(
                    bytes,
                    Platform.BYTE_ARRAY_OFFSET,
                    bytes2,
                    Platform.BYTE_ARRAY_OFFSET,
                    bytes.length);
      }
      long t = System.nanoTime();
      for (int i = 0; i < nums; i++) {
        eq =
            bytes.length == bytes2.length
                && Platform.arrayEquals(
                    bytes,
                    Platform.BYTE_ARRAY_OFFSET,
                    bytes2,
                    Platform.BYTE_ARRAY_OFFSET,
                    bytes.length);
      }
      long duration = System.nanoTime() - t;
      System.out.format("native cost %sns %sms\n", duration, duration / 1000_000);
    }
    {
      // warm
      for (int i = 0; i < nums; i++) {
        eq = Arrays.equals(bytes, bytes2);
      }
      long t = System.nanoTime();
      for (int i = 0; i < nums; i++) {
        eq = Arrays.equals(bytes, bytes2);
      }
      long duration = System.nanoTime() - t;
      System.out.format("Arrays.equals cost %sns %sms\n", duration, duration / 1000_000);
    }
    return eq;
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
