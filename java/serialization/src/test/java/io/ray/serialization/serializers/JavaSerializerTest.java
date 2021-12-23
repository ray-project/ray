package io.ray.serialization.serializers;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

import com.google.common.collect.ImmutableSet;
import io.ray.serialization.Fury;
import io.ray.serialization.serializers.JavaSerializers.CollectionJavaSerializer;
import io.ray.serialization.util.MemoryBuffer;
import io.ray.serialization.util.MemoryUtils;
import org.testng.annotations.Test;

public class JavaSerializerTest {

  @SuppressWarnings("unchecked")
  @Test
  public void testJavaSerialization() {
    ImmutableSet<Integer> set = ImmutableSet.of(1, 2, 3);
    Class<? extends ImmutableSet> setClass = set.getClass();
    Fury fury = Fury.builder().withReferenceTracking(false).build();
    MemoryBuffer buffer = MemoryUtils.buffer(32);
    CollectionJavaSerializer javaSerializer = new CollectionJavaSerializer(fury, setClass);
    javaSerializer.write(fury, buffer, set);
    Object read = javaSerializer.read(fury, buffer, setClass);
    assertEquals(set, read);

    assertSame(
        fury.getClassResolver().getSerializer(setClass).getClass(), CollectionJavaSerializer.class);
    buffer.writerIndex(0);
    buffer.readerIndex(0);
    assertEquals(set, fury.deserialize(fury.serialize(buffer, set)));
  }
}
