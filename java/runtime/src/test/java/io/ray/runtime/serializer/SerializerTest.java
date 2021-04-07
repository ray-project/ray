package io.ray.runtime.serializer;

import java.math.BigInteger;
import java.util.ArrayList;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SerializerTest {

  @Test
  public void testBasicSerialization() {
    // Test serialize / deserialize primitive types with type conversion.
    {
      Object[] foo =
          new Object[] {"hello", (byte) 1, 2.0, (short) 3, 4, 5L, new String[] {"hello", "world"}};
      Pair<byte[], Boolean> serialized = Serializer.encode(foo);
      Object[] bar = Serializer.decode(serialized.getLeft(), Object[].class);
      Assert.assertTrue(serialized.getRight());
      Assert.assertEquals(foo[0], bar[0]);
      Assert.assertEquals(((Number) foo[1]).byteValue(), ((Number) bar[1]).byteValue());
      Assert.assertEquals(foo[2], bar[2]);
      Assert.assertEquals(((Number) foo[3]).intValue(), ((Number) bar[3]).intValue());
      Assert.assertEquals(((Number) foo[4]).intValue(), ((Number) bar[4]).intValue());
      Assert.assertEquals(((Number) foo[5]).intValue(), ((Number) bar[5]).intValue());
    }
    // Test multidimensional array.
    {
      Object[][] foo = new Object[][] {{1, 2}, {"3", 4}};
      Assert.expectThrows(
          RuntimeException.class,
          () -> {
            Object[][] bar = Serializer.decode(Serializer.encode(foo).getLeft(), Integer[][].class);
          });
      Pair<byte[], Boolean> serialized = Serializer.encode(foo);
      Object[][] bar = Serializer.decode(serialized.getLeft(), Object[][].class);
      Assert.assertTrue(serialized.getRight());
      Assert.assertEquals(((Number) foo[0][1]).intValue(), ((Number) bar[0][1]).intValue());
      Assert.assertEquals(foo[1][0], bar[1][0]);
    }
    // Test List.
    {
      ArrayList<String> foo = new ArrayList<>();
      foo.add("1");
      foo.add("2");
      Pair<byte[], Boolean> serialized = Serializer.encode(foo);
      ArrayList<String> bar = Serializer.decode(serialized.getLeft(), String[].class);
      Assert.assertFalse(serialized.getRight());
      Assert.assertEquals(foo.get(0), bar.get(0));
    }
    // Test BigInteger.
    {
      BigInteger bi = BigInteger.valueOf(Long.MAX_VALUE);
      Pair<byte[], Boolean> serialized = Serializer.encode(bi);
      BigInteger newBi = Serializer.decode(serialized.getLeft(), BigInteger.class);
      Assert.assertTrue(serialized.getRight());
      Assert.assertEquals(bi, newBi);
      bi = bi.pow(2);
      serialized = Serializer.encode(bi);
      newBi = Serializer.decode(serialized.getLeft(), BigInteger.class);
      Assert.assertFalse(serialized.getRight());
      Assert.assertEquals(bi, newBi);
    }
  }
}
