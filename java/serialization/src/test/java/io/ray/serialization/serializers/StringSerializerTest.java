package io.ray.serialization.serializers;

import com.google.common.base.Strings;
import io.ray.serialization.Fury;
import io.ray.serialization.util.MemoryBuffer;
import io.ray.serialization.util.MemoryUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class StringSerializerTest {

  @Test
  public void testJavaString() {
    Fury fury = Fury.builder().withReferenceTracking(false).build();
    MemoryBuffer buffer = MemoryUtils.buffer(32);
    StringSerializer serializer = new StringSerializer(fury);
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
