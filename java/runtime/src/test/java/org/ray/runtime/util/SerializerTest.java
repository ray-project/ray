package org.ray.runtime.util;

import org.ray.runtime.serializer.Serializer;
import org.ray.runtime.serializer.CrossTypeManager;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;

class B {
  private int i;

  public B(int i) {
    this.i = i;
  }

  public static int crossTypeId() {
    return 2;
  }

  public Object[] toCrossData() {
    return new Object[]{this.i};
  }

  public static B fromCrossData(Object[] data) {
    return new B(((Number) data[0]).intValue());
  }
}

class A {
  private B b;
  private String s;

  public A(int i, String s) {
    this.b = new B(i);
    this.s = s;
  }

  private A(B b, String s) {
    this.b = b;
    this.s = s;
  }

  public static int crossTypeId() {
    return 1;
  }

  public Object[] toCrossData() {
    return new Object[]{this.b, this.s};
  }

  public static A fromCrossData(Object[] data) {
    return new A((B) data[0], (String) data[1]);
  }
}

public class SerializerTest {
  @Test
  public void testBasicSerialize() {
    {
      Serializer.Meta meta = new Serializer.Meta();
      Object[] foo = new Object[]{"hello", (byte) 1, 2.0, (short) 3, 4, 5L, new String[]{"hello", "world"}};
      Object[] bar = Serializer.decode(Serializer.encode(foo, meta), Object[].class);
      Assert.assertTrue(meta.isCrossLanguage);
      Assert.assertEquals(foo[0], bar[0]);
      Assert.assertEquals(((Number) foo[1]).byteValue(), ((Number) bar[1]).byteValue());
      Assert.assertEquals(foo[2], bar[2]);
      Assert.assertEquals(((Number) foo[3]).intValue(), ((Number) bar[3]).intValue());
      Assert.assertEquals(((Number) foo[4]).intValue(), ((Number) bar[4]).intValue());
      Assert.assertEquals(((Number) foo[5]).intValue(), ((Number) bar[5]).intValue());

    }
    {
      Serializer.Meta meta = new Serializer.Meta();
      Object[][] foo = new Object[][]{{1, 2}, {"3", 4}};
      Assert.expectThrows(RuntimeException.class, () -> {
        Object[][] bar = Serializer.decode(Serializer.encode(foo, meta), Integer[][].class);
      });
      Object[][] bar = Serializer.decode(Serializer.encode(foo, meta), Object[][].class);
      Assert.assertTrue(meta.isCrossLanguage);
      Assert.assertEquals(((Number) foo[0][1]).intValue(), ((Number) bar[0][1]).intValue());
      Assert.assertEquals(foo[1][0], bar[1][0]);
    }
    {
      Serializer.Meta meta = new Serializer.Meta();
      ArrayList<String> foo = new ArrayList<>();
      foo.add("1");
      foo.add("2");
      ArrayList<String> bar = Serializer.decode(Serializer.encode(foo, meta), String[].class);
      Assert.assertFalse(meta.isCrossLanguage);
      Assert.assertEquals(foo.get(0), bar.get(0));
    }
    {
      Serializer.Meta meta = new Serializer.Meta();
      CrossTypeManager.register(A.class);
      CrossTypeManager.register(B.class);
      A foo = new A(1, "2");
      A bar = Serializer.decode(Serializer.encode(foo, meta), Object.class);
      Assert.assertTrue(meta.isCrossLanguage);
      Assert.assertEquals(((B) foo.toCrossData()[0]).toCrossData()[0], ((B) bar.toCrossData()[0]).toCrossData()[0]);
      Assert.assertEquals(foo.toCrossData()[1], bar.toCrossData()[1]);
    }
  }
}
