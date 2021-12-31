package io.ray.runtime.serialization.serializers;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

import com.google.common.collect.ImmutableSet;
import io.ray.runtime.io.MemoryBuffer;
import io.ray.runtime.serialization.RaySerde;
import io.ray.runtime.serialization.serializers.JavaSerializers.CollectionJavaSerializer;
import org.testng.annotations.Test;

public class JavaSerializerTest {

  @SuppressWarnings("unchecked")
  @Test
  public void testJavaSerialization() {
    ImmutableSet<Integer> set = ImmutableSet.of(1, 2, 3);
    Class<? extends ImmutableSet> setClass = set.getClass();
    RaySerde raySerDe = RaySerde.builder().withReferenceTracking(false).build();
    MemoryBuffer buffer = MemoryBuffer.newHeapBuffer(32);
    CollectionJavaSerializer javaSerializer = new CollectionJavaSerializer(raySerDe, setClass);
    javaSerializer.write(buffer, set);
    Object read = javaSerializer.read(buffer);
    assertEquals(set, read);

    assertSame(
        raySerDe.getClassResolver().getSerializer(setClass).getClass(),
        CollectionJavaSerializer.class);
    buffer.writerIndex(0);
    buffer.readerIndex(0);
    assertEquals(set, raySerDe.deserialize(raySerDe.serialize(buffer, set)));
  }
}
