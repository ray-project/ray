package io.ray.serve.poll;

import org.testng.Assert;
import org.testng.annotations.Test;

public class LongPollNamespaceTest {

  @Test
  public void toStringTest() {
    String key = LongPollNamespace.ROUTE_TABLE.toString();
    Assert.assertEquals(key, "LongPollNamespace.ROUTE_TABLE");
  }

  @Test
  public void parseFromTest() {
    String key = "ROUTE_TABLE";
    LongPollNamespace longPollNamespace = LongPollNamespace.parseFrom(key);
    Assert.assertEquals(longPollNamespace, LongPollNamespace.ROUTE_TABLE);

    String unknown = "unknown";
    longPollNamespace = LongPollNamespace.parseFrom(unknown);
    Assert.assertNull(longPollNamespace);
  }
}
