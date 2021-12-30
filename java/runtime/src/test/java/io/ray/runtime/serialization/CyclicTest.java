package io.ray.runtime.serialization;

import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CyclicTest {
  @Test
  public void testBean() {
    RaySerde raySerDe = RaySerde.builder().withReferenceTracking(true).build();
    ComplexObjects.Cyclic notCyclic = ComplexObjects.Cyclic.create(true);
    Assert.assertEquals(notCyclic, raySerDe.deserialize(raySerDe.serialize(notCyclic)));
    ComplexObjects.Cyclic cyclic = ComplexObjects.Cyclic.create(true);
    Assert.assertEquals(cyclic, raySerDe.deserialize(raySerDe.serialize(cyclic)));
    Object[] arr = new Object[2];
    arr[0] = arr;
    arr[1] = cyclic;
    Assert.assertEquals(arr[1], ((Object[]) raySerDe.deserialize(raySerDe.serialize(arr)))[1]);
    List<Object> list = new ArrayList<>();
    list.add(list);
    list.add(cyclic);
    list.add(arr);
    Assert.assertEquals(
        ((Object[]) list.get(2))[1],
        ((Object[]) ((List) raySerDe.deserialize(raySerDe.serialize(list))).get(2))[1]);
  }
}
