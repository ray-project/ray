package io.ray.runtime.serialization.resolver;

import io.ray.runtime.io.MemoryBuffer;
import io.ray.runtime.serialization.ComplexObjects;
import io.ray.runtime.serialization.RaySerde;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ClassResolverTest {

  public abstract static class A implements List {}

  public abstract static class B implements Map {}

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
      List<ComplexObjects.Foo> fooList =
          Arrays.asList(ComplexObjects.Foo.create(), ComplexObjects.Foo.create());
      Assert.assertEquals(raySerDe.deserialize(raySerDe.serialize(fooList)), fooList);
      Assert.assertEquals(raySerDe.deserialize(raySerDe.serialize(fooList)), fooList);
    }
  }
}
