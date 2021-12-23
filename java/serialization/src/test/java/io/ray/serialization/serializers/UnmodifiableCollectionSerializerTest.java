package io.ray.serialization.serializers;

import static org.testng.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import io.ray.serialization.Fury;
import io.ray.serialization.serializers.UnmodifiableCollectionSerializer.UnmodifiableCollection;
import io.ray.serialization.util.MemoryBuffer;
import io.ray.serialization.util.MemoryUtils;
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
    Fury fury = Fury.builder().build();
    MemoryBuffer buffer = MemoryUtils.buffer(32);
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
              fury,
              ((Class) value.getClass()),
              UnmodifiableCollection.valueOfType(value.getClass()));
      serializer.write(fury, buffer, value);
      Object newObj = serializer.read(fury, buffer, ((Class) value.getClass()));
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
