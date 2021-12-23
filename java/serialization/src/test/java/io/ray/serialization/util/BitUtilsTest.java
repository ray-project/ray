package io.ray.serialization.util;

import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class BitUtilsTest {

  @Test
  public void anyUnSet() {
    int valueCount = 10;
    MemoryBuffer buffer = MemoryUtils.buffer(valueCount);
    int offset = 0;
    for (int i = 0; i < 67; i++) {
      BitUtils.set(buffer, 0, offset++);
    }
    for (int i = 0; i < offset; i++) {
      assertTrue(BitUtils.isSet(buffer, 0, i));
    }
  }
}
