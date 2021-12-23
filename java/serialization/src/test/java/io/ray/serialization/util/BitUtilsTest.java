package io.ray.serialization.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import org.testng.annotations.Test;

public class BitUtilsTest {

  @Test
  public void anyUnSet() {
    int valueCount = 10;
    MemoryBuffer buffer = MemoryUtils.buffer(valueCount);
    int i = 0;
    BitUtils.set(buffer, 0, i++);
    BitUtils.set(buffer, 0, i++);
    BitUtils.set(buffer, 0, i++);
    BitUtils.set(buffer, 0, i++);
    BitUtils.set(buffer, 0, i++);
    BitUtils.set(buffer, 0, i++);
    BitUtils.set(buffer, 0, i++);
    BitUtils.set(buffer, 0, i++);
    BitUtils.set(buffer, 0, i++);
    BitUtils.set(buffer, 0, i++);
    assertFalse(BitUtils.anyUnSet(buffer, 0, valueCount));
    StringUtils.encodeHexString(buffer.getRemainingBytes());
  }

  @Test
  public void getNullCount() {
    int valueCount = 14;
    MemoryBuffer buffer = MemoryUtils.buffer(valueCount);
    buffer.put(0, (byte) 0b11000000);
    assertEquals(BitUtils.getNullCount(buffer, 0, 8), 6);
  }

  @Test
  public void testSetAll() {
    int valueCount = 10;
    MemoryBuffer buffer = MemoryUtils.buffer(8);
    BitUtils.setAll(buffer, 0, valueCount);
    assertEquals(BitUtils.getNullCount(buffer, 0, valueCount), 0);
    assertEquals("ff03000000000000", StringUtils.encodeHexString(buffer.getRemainingBytes()));
  }
}
