package io.ray.runtime.io;

import static org.testng.Assert.assertEquals;

import java.nio.ByteBuffer;
import org.testng.annotations.Test;

public class MockWritableByteChannelTest {

  @Test
  public void testWrite() {
    try (MockWritableByteChannel channel = new MockWritableByteChannel()) {
      channel.write(ByteBuffer.allocate(100));
      assertEquals(channel.totalBytes(), 100);
    }
  }
}
