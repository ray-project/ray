package io.ray.runtime.serialization.serializers;

import static org.testng.Assert.assertEquals;

import com.google.common.base.Preconditions;
import io.ray.runtime.io.MemoryBuffer;
import io.ray.runtime.serialization.ComplexObjects;
import io.ray.runtime.serialization.RaySerde;
import lombok.Data;
import org.testng.annotations.Test;

@SuppressWarnings("unchecked")
public class DefaultSerializerTest {

  @Test
  public void testLocalClass() {
    String str = "str";
    class Foo {
      public String foo(String s) {
        return str + s;
      }
    }
    RaySerde raySerDe = RaySerde.builder().withReferenceTracking(false).build();
    DefaultSerializer serializer = new DefaultSerializer(raySerDe, Foo.class);
    MemoryBuffer buffer = MemoryBuffer.newHeapBuffer(32);
    Foo foo = new Foo();
    serializer.write(raySerDe, buffer, foo);
    Object obj = serializer.read(raySerDe, buffer, Foo.class);
    assertEquals(foo.foo("str"), ((Foo) obj).foo("str"));
  }

  @Test
  public void testAnonymousClass() {
    String str = "str";
    class Foo {
      public String foo(String s) {
        return str + s;
      }
    }
    Foo foo =
        new Foo() {
          @Override
          public String foo(String s) {
            return "Anonymous " + s;
          }
        };
    RaySerde raySerDe = RaySerde.builder().withReferenceTracking(false).build();
    DefaultSerializer serializer = new DefaultSerializer(raySerDe, foo.getClass());
    MemoryBuffer buffer = MemoryBuffer.newHeapBuffer(32);
    serializer.write(raySerDe, buffer, foo);
    Object obj = serializer.read(raySerDe, buffer, foo.getClass());
    assertEquals(foo.foo("str"), ((Foo) obj).foo("str"));
  }

  @Test
  public void testSerializeCircularReference() {
    ComplexObjects.Cyclic cyclic = ComplexObjects.Cyclic.create(true);
    RaySerde raySerDe = RaySerde.builder().withReferenceTracking(true).build();
    MemoryBuffer buffer = MemoryBuffer.newHeapBuffer(32);

    DefaultSerializer<ComplexObjects.Cyclic> serializer =
        new DefaultSerializer<>(raySerDe, ComplexObjects.Cyclic.class);
    raySerDe.getReferenceResolver().writeReferenceOrNull(buffer, cyclic);
    serializer.write(raySerDe, buffer, cyclic);
    byte tag = raySerDe.getReferenceResolver().readReferenceOrNull(buffer);
    Preconditions.checkArgument(tag == RaySerde.NOT_NULL);
    raySerDe.getReferenceResolver().preserveReferenceId();
    ComplexObjects.Cyclic cyclic1 = serializer.read(raySerDe, buffer, ComplexObjects.Cyclic.class);
    raySerDe.reset();
    assertEquals(cyclic1, cyclic);
  }

  @Data
  public static class A {
    Integer f1;
    Integer f2;
    Long f3;
    int f4;
    int f5;
    Integer f6;
    Long f7;
  }

  @Test
  public void testSerialization() {
    RaySerde raySerDe = RaySerde.builder().withReferenceTracking(false).build();
    MemoryBuffer buffer = MemoryBuffer.newHeapBuffer(32);
    DefaultSerializer<A> serializer = new DefaultSerializer<>(raySerDe, A.class);
    A a = new A();
    serializer.write(raySerDe, buffer, a);
    assertEquals(a, serializer.read(raySerDe, buffer, A.class));
  }

  static class B {
    int f1;
  }

  static class C extends B {
    int f1;
  }

  @Test()
  public void testDuplicateFields() {
    C c = new C();
    ((B) c).f1 = 100;
    c.f1 = -100;
    assertEquals(((B) c).f1, 100);
    assertEquals(c.f1, -100);
    RaySerde raySerDe = RaySerde.builder().withReferenceTracking(false).build();
    DefaultSerializer<C> serializer = new DefaultSerializer<>(raySerDe, C.class);
    MemoryBuffer buffer = MemoryBuffer.newHeapBuffer(32);
    serializer.write(raySerDe, buffer, c);
    C newC = serializer.read(raySerDe, buffer, C.class);
    assertEquals(newC.f1, c.f1);
    assertEquals(((B) newC).f1, ((B) c).f1);
  }
}
