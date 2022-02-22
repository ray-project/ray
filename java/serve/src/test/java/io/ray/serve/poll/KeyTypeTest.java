package io.ray.serve.poll;

import org.testng.Assert;
import org.testng.annotations.Test;

public class KeyTypeTest {

  @Test
  public void hashTest() {
    KeyType k1 = new KeyType(LongPollNamespace.ROUTE_TABLE, "k1");
    KeyType k2 = new KeyType(LongPollNamespace.ROUTE_TABLE, "k1");
    KeyType k3 = new KeyType(LongPollNamespace.ROUTE_TABLE, null);
    KeyType k4 = new KeyType(LongPollNamespace.ROUTE_TABLE, "k4");

    Assert.assertEquals(k1, k1);
    Assert.assertEquals(k1.hashCode(), k1.hashCode());
    Assert.assertTrue(k1.equals(k1));

    Assert.assertEquals(k1, k2);
    Assert.assertEquals(k1.hashCode(), k2.hashCode());
    Assert.assertTrue(k1.equals(k2));

    Assert.assertNotEquals(k1, k3);
    Assert.assertNotEquals(k1.hashCode(), k3.hashCode());
    Assert.assertFalse(k1.equals(k3));

    Assert.assertNotEquals(k1, k4);
    Assert.assertNotEquals(k1.hashCode(), k4.hashCode());
    Assert.assertFalse(k1.equals(k4));
  }

  @Test
  public void toStringTest() {
    KeyType k1 = new KeyType(LongPollNamespace.ROUTE_TABLE, "k1");
    Assert.assertEquals(k1.toString(), "(LongPollNamespace.ROUTE_TABLE, k1)");

    KeyType k2 = new KeyType(LongPollNamespace.ROUTE_TABLE, null);
    Assert.assertEquals(k2.toString(), "LongPollNamespace.ROUTE_TABLE");
  }

  @Test
  public void parseFromTest() {
    String key1 = "(LongPollNamespace.ROUTE_TABLE, k1)";
    KeyType k1 = KeyType.parseFrom(key1);
    Assert.assertEquals(k1.getLongPollNamespace(), LongPollNamespace.ROUTE_TABLE);
    Assert.assertEquals(k1.getKey(), "k1");

    String key2 = "LongPollNamespace.ROUTE_TABLE";
    KeyType k2 = KeyType.parseFrom(key2);
    Assert.assertEquals(k2.getLongPollNamespace(), LongPollNamespace.ROUTE_TABLE);
    Assert.assertNull(k2.getKey());

    String key3 = "LongPollNamespace.unknown";
    try {
      KeyType.parseFrom(key3);
      Assert.assertTrue(false, "Expect exception here!");
    } catch (Exception e) {
    }
  }
}
