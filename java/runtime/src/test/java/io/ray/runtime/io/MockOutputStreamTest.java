package io.ray.runtime.io;

import static org.testng.Assert.assertEquals;

import java.nio.ByteBuffer;
import org.testng.annotations.Test;

public class MockOutputStreamTest {

  @Test
  public void testWrite() throws Exception {
    try (MockOutputStream stream = new MockOutputStream()) {
      stream.write(1);
      stream.write(ByteBuffer.allocate(100), 10);
      stream.write(new byte[100], 10, 10);
      stream.write(new byte[100]);
      assertEquals(stream.totalBytes(), 1 + 10 + 10 + 100);
    }
  }
}
