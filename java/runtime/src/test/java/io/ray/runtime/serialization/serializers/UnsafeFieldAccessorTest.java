package io.ray.runtime.serialization.serializers;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public class UnsafeFieldAccessorTest {

  class A {
    int f1 = 1;
    int f2 = 2;
  }

  class B extends A {
    int f1 = 2;
  }

  @Test
  public void testRepeatedFields() {
    assertEquals(new UnsafeFieldAccessor(A.class, "f1").getInt(new A()), 1);
    assertEquals(new UnsafeFieldAccessor(A.class, "f2").getInt(new A()), 2);
    assertEquals(new UnsafeFieldAccessor(B.class, "f1").getInt(new B()), 2);
    assertEquals(new UnsafeFieldAccessor(B.class, "f2").getInt(new B()), 2);
  }
}
