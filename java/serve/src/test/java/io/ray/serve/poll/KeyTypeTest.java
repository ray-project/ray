package io.ray.serve.poll;

import org.testng.Assert;
import org.testng.annotations.Test;

public class KeyTypeTest {

  @Test
  public void equalsTest() {
    KeyType k1 = new KeyType(LongPollNamespace.ROUTE_TABLE, "k1");
    KeyType k2 = new KeyType(LongPollNamespace.ROUTE_TABLE, "k1");
    KeyType k3 = new KeyType(LongPollNamespace.ROUTE_TABLE, null);
    KeyType k4 = new KeyType(LongPollNamespace.ROUTE_TABLE, "k4");

    Assert.assertTrue(k1.equals(k1));
    Assert.assertTrue(k1.equals(k2));
    Assert.assertTrue(k2.equals(k1));
    Assert.assertFalse(k1.equals(k3));
    Assert.assertFalse(k3.equals(k1));
    Assert.assertFalse(k1.equals(k4));
    Assert.assertFalse(k4.equals(k1));
  }

  @Test
  public void hashCodeTest() {
    KeyType k1 = new KeyType(LongPollNamespace.ROUTE_TABLE, "k1");
    KeyType k2 = new KeyType(LongPollNamespace.ROUTE_TABLE, "k1");
    KeyType k3 = new KeyType(LongPollNamespace.ROUTE_TABLE, null);
    KeyType k4 = new KeyType(LongPollNamespace.ROUTE_TABLE, "k4");

    Assert.assertEquals(k1.hashCode(), k2.hashCode());
    Assert.assertNotEquals(k1.hashCode(), k3.hashCode());
    Assert.assertNotEquals(k1.hashCode(), k4.hashCode());
  }

  @Test
  public void toStringTest() {
    KeyType k1 = new KeyType(LongPollNamespace.ROUTE_TABLE, "k1");
    Assert.assertEquals(k1.toString(), "(ROUTE_TABLE,k1)");

    KeyType k2 = new KeyType(LongPollNamespace.ROUTE_TABLE, null);
    Assert.assertEquals(k2.toString(), "ROUTE_TABLE");

    KeyType k3 = new KeyType(null, "k3");
    Assert.assertEquals(k3.toString(), "k3");

    KeyType k4 = new KeyType(null, null);
    Assert.assertEquals(k4.toString(), "");
  }

  @Test
  public void parseFromTest() {
    KeyType k1 = KeyType.parseFrom("(ROUTE_TABLE,k1)");
    Assert.assertEquals(k1.getLongPollNamespace(), LongPollNamespace.ROUTE_TABLE);
    Assert.assertEquals(k1.getKey(), "k1");

    KeyType k2 = KeyType.parseFrom("ROUTE_TABLE");
    Assert.assertEquals(k2.getLongPollNamespace(), LongPollNamespace.ROUTE_TABLE);
    Assert.assertNull(k2.getKey());

    String key3 = "k3";
    KeyType k3 = KeyType.parseFrom(key3);
    Assert.assertEquals(k3.getKey(), key3);
    Assert.assertNull(k3.getLongPollNamespace());

    KeyType k4 = KeyType.parseFrom("");
    Assert.assertNull(k4.getKey());
    Assert.assertNull(k4.getLongPollNamespace());
  }
}
