package io.ray.streaming.runtime.util;

import java.io.Serializable;
import java.util.Collections;
import org.testng.annotations.Test;
import static org.testng.Assert.assertEquals;

public class ReflectionUtilsTest {

  static class Foo implements Serializable {
    public void f1() {
    }

    public void f2() {
    }

    public void f2(boolean a1) {
    }
  }

  @Test
  public void testFindMethod() throws NoSuchMethodException {
    assertEquals(Foo.class.getDeclaredMethod("f1"),
        ReflectionUtils.findMethod(Foo.class, "f1"));
  }

  @Test
  public void testFindMethods() {
    assertEquals(ReflectionUtils.findMethods(Foo.class, "f2").size(), 2);
  }

  @Test
  public void testGetAllInterfaces() {
    assertEquals(ReflectionUtils.getAllInterfaces(Foo.class),
        Collections.singletonList(Serializable.class));
  }
}