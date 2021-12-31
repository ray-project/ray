package io.ray.runtime.serialization.serializers;

import static org.testng.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import io.ray.runtime.io.MemoryBuffer;
import io.ray.runtime.serialization.RaySerde;
import io.ray.runtime.serialization.serializers.UnmodifiableCollectionSerializer.UnmodifiableCollection;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.TreeMap;
import java.util.TreeSet;
import org.testng.annotations.Test;

public class UnmodifiableCollectionSerializerTest {

  @SuppressWarnings("unchecked")
  @Test
  public void testWrite() throws Exception {
    RaySerde raySerDe = RaySerde.builder().build();
    MemoryBuffer buffer = MemoryBuffer.newHeapBuffer(32);
    Object[] values =
        new Object[] {
          Collections.unmodifiableCollection(Collections.singletonList("abc")),
          Collections.unmodifiableList(Arrays.asList("abc", "def")),
          Collections.unmodifiableList(new LinkedList<>(Arrays.asList("abc", "def"))),
          Collections.unmodifiableSet(new HashSet<>(Arrays.asList("abc", "def"))),
          Collections.unmodifiableSortedSet(new TreeSet<>(Arrays.asList("abc", "def"))),
          Collections.unmodifiableMap(ImmutableMap.of("k1", "v1")),
          Collections.unmodifiableSortedMap(new TreeMap<>(ImmutableMap.of("k1", "v1")))
        };
    for (Object value : values) {
      buffer.writerIndex(0);
      buffer.readerIndex(0);
      UnmodifiableCollectionSerializer serializer =
          new UnmodifiableCollectionSerializer(
              raySerDe,
              ((Class) value.getClass()),
              UnmodifiableCollection.valueOfType(value.getClass()));
      serializer.write(buffer, value);
      Object newObj = serializer.read(buffer);
      assertEquals(newObj.getClass(), value.getClass());
      UnmodifiableCollection unmodifiableCollection =
          UnmodifiableCollection.valueOfType(newObj.getClass());
      Field field = UnmodifiableCollection.class.getDeclaredField("sourceCollectionField");
      field.setAccessible(true);
      Field sourceCollectionField = (Field) field.get(unmodifiableCollection);
      Object innerValue = sourceCollectionField.get(value);
      assertEquals(innerValue, sourceCollectionField.get(newObj));
    }
  }
}
