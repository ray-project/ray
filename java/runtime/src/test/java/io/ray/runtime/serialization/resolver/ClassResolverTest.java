package io.ray.runtime.serialization.resolver;

import static org.testng.Assert.assertEquals;

import io.ray.runtime.io.MemoryBuffer;
import io.ray.runtime.serialization.ComplexObjects;
import io.ray.runtime.serialization.RaySerde;
import io.ray.runtime.serialization.serializers.CollectionSerializers.ArrayListSerializer;
import io.ray.runtime.serialization.serializers.CollectionSerializers.CollectionDefaultJavaSerializer;
import io.ray.runtime.serialization.serializers.CollectionSerializers.CollectionSerializer;
import io.ray.runtime.serialization.serializers.CollectionSerializers.HashSetSerializer;
import io.ray.runtime.serialization.serializers.CollectionSerializers.SortedSetSerializer;
import io.ray.runtime.serialization.serializers.JavaSerializers.CollectionJavaSerializer;
import io.ray.runtime.serialization.serializers.JavaSerializers.JavaSerializer;
import io.ray.runtime.serialization.serializers.JavaSerializers.MapJavaSerializer;
import io.ray.runtime.serialization.serializers.MapSerializers.HashMapSerializer;
import io.ray.runtime.serialization.serializers.MapSerializers.MapDefaultJavaSerializer;
import io.ray.runtime.serialization.serializers.MapSerializers.SortedMapSerializer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ClassResolverTest {

  public abstract static class A implements List {}

  public abstract static class B implements Map {}

  @Test
  public void testGetSerializerClass() {
    RaySerde raySerDe = RaySerde.builder().build();
    ClassResolver classResolver = raySerDe.getClassResolver();
    assertEquals(classResolver.getSerializerClass(ArrayList.class), ArrayListSerializer.class);
    assertEquals(classResolver.getSerializerClass(LinkedList.class), CollectionSerializer.class);

    assertEquals(classResolver.getSerializerClass(HashSet.class), HashSetSerializer.class);
    assertEquals(classResolver.getSerializerClass(LinkedHashSet.class), HashSetSerializer.class);
    assertEquals(classResolver.getSerializerClass(TreeSet.class), SortedSetSerializer.class);

    assertEquals(classResolver.getSerializerClass(HashMap.class), HashMapSerializer.class);
    assertEquals(classResolver.getSerializerClass(LinkedHashMap.class), HashMapSerializer.class);
    assertEquals(classResolver.getSerializerClass(TreeMap.class), SortedMapSerializer.class);

    if (JavaSerializer.requireJavaSerialization(ArrayBlockingQueue.class)) {
      assertEquals(
          classResolver.getSerializerClass(ArrayBlockingQueue.class),
          CollectionJavaSerializer.class);
    } else {
      assertEquals(
          classResolver.getSerializerClass(ArrayBlockingQueue.class),
          CollectionDefaultJavaSerializer.class);
    }
    if (JavaSerializer.requireJavaSerialization(ConcurrentHashMap.class)) {
      assertEquals(
          classResolver.getSerializerClass(ConcurrentHashMap.class), MapJavaSerializer.class);
    } else {
      assertEquals(
          classResolver.getSerializerClass(ConcurrentHashMap.class),
          MapDefaultJavaSerializer.class);
    }

    assertEquals(classResolver.getSerializerClass(A.class), CollectionDefaultJavaSerializer.class);
    assertEquals(classResolver.getSerializerClass(B.class), MapDefaultJavaSerializer.class);
  }

  @Test
  public void testWriteClassName() {
    {
      RaySerde raySerDe = RaySerde.builder().withReferenceTracking(true).build();
      ClassResolver classResolver = raySerDe.getClassResolver();
      MemoryBuffer buffer = MemoryBuffer.newHeapBuffer(32);
      classResolver.writeClassNameBytes(buffer, getClass());
      int writerIndex = buffer.writerIndex();
      classResolver.writeClassNameBytes(buffer, getClass());
      Assert.assertEquals(buffer.writerIndex(), writerIndex + 3);
      buffer.writerIndex(0);
    }
    {
      RaySerde raySerDe = RaySerde.builder().withReferenceTracking(true).build();
      ClassResolver classResolver = raySerDe.getClassResolver();
      MemoryBuffer buffer = MemoryBuffer.newHeapBuffer(32);
      classResolver.writeClass(buffer, getClass());
      classResolver.writeClass(buffer, getClass());
      Assert.assertSame(classResolver.readClass(buffer), getClass());
      Assert.assertSame(classResolver.readClass(buffer), getClass());
      classResolver.reset();
      buffer.writerIndex(0);
      buffer.readerIndex(0);
      List<ComplexObjects.Foo> fooList = Arrays.asList(ComplexObjects.Foo.create(), ComplexObjects.Foo.create());
      Assert.assertEquals(raySerDe.deserialize(raySerDe.serialize(fooList)), fooList);
      Assert.assertEquals(raySerDe.deserialize(raySerDe.serialize(fooList)), fooList);
    }
  }
}
