package io.ray.runtime.serialization.serializers;

import com.google.common.base.Strings;
import io.ray.runtime.io.MemoryBuffer;
import io.ray.runtime.serialization.RaySerde;
import org.testng.Assert;
import org.testng.annotations.Test;

public class StringSerializerTest {

  @Test
  public void testJavaString() {
    RaySerde raySerDe = RaySerde.builder().withReferenceTracking(false).build();
    MemoryBuffer buffer = MemoryBuffer.newHeapBuffer(32);
    StringSerializer serializer = new StringSerializer(raySerDe);
    String str = "str";
    serializer.writeJavaString(buffer, str);
    Assert.assertEquals(str, serializer.readJavaString(buffer));

    String longStr = Strings.repeat("abc", 50);
    buffer.writerIndex(0);
    buffer.readerIndex(0);
    serializer.writeJavaString(buffer, longStr);
    Assert.assertEquals(longStr, serializer.readJavaString(buffer));
  }
}
