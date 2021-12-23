package io.ray.serialization;

import io.ray.serialization.bean.Cyclic;
import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CyclicTest {
  @Test
  public void testBean() {
    Fury fury = Fury.builder().withReferenceTracking(true).build();
    Cyclic notCyclic = Cyclic.create(true);
    Assert.assertEquals(notCyclic, fury.deserialize(fury.serialize(notCyclic)));
    Cyclic cyclic = Cyclic.create(true);
    Assert.assertEquals(cyclic, fury.deserialize(fury.serialize(cyclic)));
    Object[] arr = new Object[2];
    arr[0] = arr;
    arr[1] = cyclic;
    Assert.assertEquals(arr[1], ((Object[]) fury.deserialize(fury.serialize(arr)))[1]);
    List<Object> list = new ArrayList<>();
    list.add(list);
    list.add(cyclic);
    list.add(arr);
    Assert.assertEquals(
        ((Object[]) list.get(2))[1],
        ((Object[]) ((List) fury.deserialize(fury.serialize(list))).get(2))[1]);
  }
}
