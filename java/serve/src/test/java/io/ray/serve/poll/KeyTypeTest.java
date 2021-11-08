package io.ray.serve.poll;

import com.google.gson.Gson;
import org.testng.Assert;
import org.testng.annotations.Test;

public class KeyTypeTest {

  private static final Gson GSON = new Gson();

  @Test
  public void hashTest() {
    KeyType k1 = new KeyType(LongPollNamespace.BACKEND_CONFIGS, "k1");
    KeyType k2 = new KeyType(LongPollNamespace.BACKEND_CONFIGS, "k1");
    KeyType k3 = new KeyType(LongPollNamespace.BACKEND_CONFIGS, null);
    KeyType k4 = new KeyType(LongPollNamespace.REPLICA_HANDLES, "k4");

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
  public void jsonTest() {
    KeyType k1 = new KeyType(LongPollNamespace.BACKEND_CONFIGS, "k1");
    String json = GSON.toJson(k1);

    KeyType k2 = GSON.fromJson(json, KeyType.class);
    Assert.assertEquals(k1, k2);
    Assert.assertEquals(k1.hashCode(), k2.hashCode());
  }
}
