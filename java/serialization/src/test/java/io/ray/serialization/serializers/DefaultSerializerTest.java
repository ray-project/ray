package io.ray.serialization.serializers;

import static org.testng.Assert.assertEquals;

import com.google.common.base.Preconditions;
import io.ray.serialization.Fury;
import io.ray.serialization.bean.Cyclic;
import io.ray.serialization.util.MemoryBuffer;
import io.ray.serialization.util.MemoryUtils;
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
    Fury fury = Fury.builder().withReferenceTracking(false).build();
    DefaultSerializer serializer = new DefaultSerializer(fury, Foo.class);
    MemoryBuffer buffer = MemoryUtils.buffer(32);
    Foo foo = new Foo();
    serializer.write(fury, buffer, foo);
    Object obj = serializer.read(fury, buffer, Foo.class);
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
    Fury fury = Fury.builder().withReferenceTracking(false).build();
    DefaultSerializer serializer = new DefaultSerializer(fury, foo.getClass());
    MemoryBuffer buffer = MemoryUtils.buffer(32);
    serializer.write(fury, buffer, foo);
    Object obj = serializer.read(fury, buffer, foo.getClass());
    assertEquals(foo.foo("str"), ((Foo) obj).foo("str"));
  }

  @Test
  public void testSerializeCircularReference() {
    Cyclic cyclic = Cyclic.create(true);
    Fury fury = Fury.builder().withReferenceTracking(true).build();
    MemoryBuffer buffer = MemoryUtils.buffer(32);

    DefaultSerializer<Cyclic> serializer = new DefaultSerializer<>(fury, Cyclic.class);
    fury.getReferenceResolver().writeReferenceOrNull(buffer, cyclic);
    serializer.write(fury, buffer, cyclic);
    byte tag = fury.getReferenceResolver().readReferenceOrNull(buffer);
    Preconditions.checkArgument(tag == Fury.NOT_NULL);
    fury.getReferenceResolver().preserveReferenceId();
    Cyclic cyclic1 = serializer.read(fury, buffer, Cyclic.class);
    fury.reset();
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
    Fury fury = Fury.builder().withReferenceTracking(false).build();
    MemoryBuffer buffer = MemoryUtils.buffer(32);
    DefaultSerializer<A> serializer = new DefaultSerializer<>(fury, A.class);
    A a = new A();
    serializer.write(fury, buffer, a);
    assertEquals(a, serializer.read(fury, buffer, A.class));
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
    Fury fury = Fury.builder().withReferenceTracking(false).build();
    DefaultSerializer<C> serializer = new DefaultSerializer<>(fury, C.class);
    MemoryBuffer buffer = MemoryUtils.buffer(32);
    serializer.write(fury, buffer, c);
    C newC = serializer.read(fury, buffer, C.class);
    assertEquals(newC.f1, c.f1);
    assertEquals(((B) newC).f1, ((B) c).f1);
  }
}
