package org.ray.runtime.util;

import org.apache.commons.lang3.tuple.Pair;
import org.ray.runtime.serializer.Serializer;
import org.ray.runtime.serializer.Serializer.Meta;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;

public class SerializerTest {

  @Test
  public void testBasicSerialize() {
    {
      Object[] foo = new Object[]{"hello", (byte) 1, 2.0, (short) 3, 4, 5L,
          new String[]{"hello", "world"}};
      Pair<byte[], Meta> serialized = Serializer.encode(foo);
      Object[] bar = Serializer.decode(serialized.getLeft(), Object[].class);
      Assert.assertTrue(serialized.getRight().isCrossLanguage);
      Assert.assertEquals(foo[0], bar[0]);
      Assert.assertEquals(((Number) foo[1]).byteValue(), ((Number) bar[1]).byteValue());
      Assert.assertEquals(foo[2], bar[2]);
      Assert.assertEquals(((Number) foo[3]).intValue(), ((Number) bar[3]).intValue());
      Assert.assertEquals(((Number) foo[4]).intValue(), ((Number) bar[4]).intValue());
      Assert.assertEquals(((Number) foo[5]).intValue(), ((Number) bar[5]).intValue());

    }
    {
      Object[][] foo = new Object[][]{{1, 2}, {"3", 4}};
      Assert.expectThrows(RuntimeException.class, () -> {
        Object[][] bar = Serializer.decode(Serializer.encode(foo).getLeft(), Integer[][].class);
      });
      Pair<byte[], Serializer.Meta> serialized = Serializer.encode(foo);
      Object[][] bar = Serializer.decode(serialized.getLeft(), Object[][].class);
      Assert.assertTrue(serialized.getRight().isCrossLanguage);
      Assert.assertEquals(((Number) foo[0][1]).intValue(), ((Number) bar[0][1]).intValue());
      Assert.assertEquals(foo[1][0], bar[1][0]);
    }
    {
      ArrayList<String> foo = new ArrayList<>();
      foo.add("1");
      foo.add("2");
      Pair<byte[], Serializer.Meta> serialized = Serializer.encode(foo);
      ArrayList<String> bar = Serializer.decode(serialized.getLeft(), String[].class);
      Assert.assertFalse(serialized.getRight().isCrossLanguage);
      Assert.assertEquals(foo.get(0), bar.get(0));
    }
  }
}
