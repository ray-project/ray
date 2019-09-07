package org.ray.api.test;

import org.ray.api.Ray;
import org.ray.api.RayPyActor;
import org.ray.api.id.ObjectId;
import org.ray.runtime.AbstractRayRuntime;
import org.ray.runtime.object.NativeRayObject;
import org.ray.runtime.object.ObjectStore;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RaySerializerTest extends BaseMultiLanguageTest {

  @Test
  public void testSerializePyActor() {
    RayPyActor pyActor = Ray.createPyActor("test", "RaySerializerTest");
    ObjectStore objectStore = ((AbstractRayRuntime) Ray.internal()).getObjectStore();
    NativeRayObject nativeRayObject = objectStore.serialize(pyActor);
    RayPyActor result = (RayPyActor) objectStore
        .deserialize(nativeRayObject, ObjectId.fromRandom());
    Assert.assertEquals(result.getId(), pyActor.getId());
    Assert.assertEquals(result.getModuleName(), "test");
    Assert.assertEquals(result.getClassName(), "RaySerializerTest");
  }

}
