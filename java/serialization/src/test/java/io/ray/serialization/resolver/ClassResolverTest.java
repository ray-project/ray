package io.ray.serialization.resolver;

import static org.testng.Assert.assertEquals;

import io.ray.serialization.Fury;
import io.ray.serialization.bean.Foo;
import io.ray.serialization.serializers.CollectionSerializers.ArrayListSerializer;
import io.ray.serialization.serializers.CollectionSerializers.ArraysAsListSerializer;
import io.ray.serialization.serializers.CollectionSerializers.CollectionDefaultJavaSerializer;
import io.ray.serialization.serializers.CollectionSerializers.CollectionSerializer;
import io.ray.serialization.serializers.CollectionSerializers.HashSetSerializer;
import io.ray.serialization.serializers.CollectionSerializers.SortedSetSerializer;
import io.ray.serialization.serializers.JavaSerializers.CollectionJavaSerializer;
import io.ray.serialization.serializers.JavaSerializers.JavaSerializer;
import io.ray.serialization.serializers.JavaSerializers.MapJavaSerializer;
import io.ray.serialization.serializers.MapSerializers.HashMapSerializer;
import io.ray.serialization.serializers.MapSerializers.MapDefaultJavaSerializer;
import io.ray.serialization.serializers.MapSerializers.SortedMapSerializer;
import io.ray.serialization.util.MemoryBuffer;
import io.ray.serialization.util.MemoryUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
    Fury fury = Fury.builder().build();
    ClassResolver classResolver = fury.getClassResolver();
    assertEquals(classResolver.getSerializerClass(ArrayList.class), ArrayListSerializer.class);
    assertEquals(
        classResolver.getSerializerClass(Arrays.asList(1, 2).getClass()),
        ArraysAsListSerializer.class);
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
      Fury fury = Fury.builder().withReferenceTracking(true).build();
      ClassResolver classResolver = fury.getClassResolver();
      MemoryBuffer buffer = MemoryUtils.buffer(32);
      classResolver.writeClassNameBytes(buffer, getClass());
      int writerIndex = buffer.writerIndex();
      classResolver.writeClassNameBytes(buffer, getClass());
      Assert.assertEquals(buffer.writerIndex(), writerIndex + 3);
      buffer.writerIndex(0);
    }
    {
      Fury fury = Fury.builder().withReferenceTracking(true).build();
      ClassResolver classResolver = fury.getClassResolver();
      MemoryBuffer buffer = MemoryUtils.buffer(32);
      classResolver.writeClass(buffer, getClass());
      classResolver.writeClass(buffer, getClass());
      Assert.assertSame(classResolver.readClass(buffer), getClass());
      Assert.assertSame(classResolver.readClass(buffer), getClass());
      classResolver.reset();
      buffer.writerIndex(0);
      buffer.readerIndex(0);
      List<Foo> fooList = Arrays.asList(Foo.create(), Foo.create());
      Assert.assertEquals(fury.deserialize(fury.serialize(fooList)), fooList);
      Assert.assertEquals(fury.deserialize(fury.serialize(fooList)), fooList);
    }
  }

  @Test
  public void testClassProvider() {
    Fury fury = Fury.builder().withReferenceTracking(true).build();
    ClassResolver classResolver = fury.getClassResolver();
    Assert.assertTrue(classResolver.getRegisteredClasses().contains(A.class));
    Assert.assertTrue(classResolver.getRegisteredClasses().contains(B.class));
  }

  public static class TestClassProvider1 implements ClassProvider {

    @Override
    public List<Class<?>> getClasses() {
      return Collections.singletonList(A.class);
    }
  }

  public static class TestClassProvider2 implements ClassProvider {

    @Override
    public List<Class<?>> getClasses() {
      return Collections.singletonList(B.class);
    }
  }
}
