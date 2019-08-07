package org.ray.api.test;

import org.ray.api.Ray;
import org.ray.api.RayPyActor;
import org.ray.api.id.ObjectId;
import org.ray.runtime.AbstractRayRuntime;
import org.ray.runtime.nativeTypes.NativeRayObject;
import org.ray.runtime.objectstore.ObjectStoreProxy;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RaySerializerTest extends BaseMultiLanguageTest {

  @Test
  public void testSerializePyActor() {
    RayPyActor pyActor = Ray.createPyActor("test", "RaySerializerTest");
    ObjectStoreProxy objectStoreProxy = ((AbstractRayRuntime) Ray.internal()).getWorker()
        .getObjectStoreProxy();
    NativeRayObject nativeRayObject = objectStoreProxy.serialize(pyActor);
    RayPyActor result = (RayPyActor) objectStoreProxy
        .deserialize(nativeRayObject, ObjectId.fromRandom());
    Assert.assertEquals(result.getId(), pyActor.getId());
    Assert.assertEquals(result.getModuleName(), "test");
    Assert.assertEquals(result.getClassName(), "RaySerializerTest");
  }

}
