package org.ray.api.test;

import org.ray.api.RayPyActor;
import org.ray.api.id.UniqueId;
import org.ray.runtime.RayPyActorImpl;
import org.ray.runtime.util.Serializer;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RaySerializerTest {

  @Test
  public void testSerializePyActor() {
    final UniqueId pyActorId = UniqueId.randomId();
    RayPyActor pyActor = new RayPyActorImpl(pyActorId, "test", "RaySerializerTest");
    byte[] bytes = Serializer.encode(pyActor);
    RayPyActor result = Serializer.decode(bytes);
    Assert.assertEquals(result.getId(), pyActorId);
    Assert.assertEquals(result.getModuleName(), "test");
    Assert.assertEquals(result.getClassName(), "RaySerializerTest");
  }

}
