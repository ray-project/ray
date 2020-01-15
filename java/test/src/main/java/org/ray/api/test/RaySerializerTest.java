package org.ray.api.test;

import org.ray.api.Ray;
import org.ray.api.RayPyActor;
import org.ray.api.TestUtils;
import org.ray.runtime.context.WorkerContext;
import org.ray.runtime.object.NativeRayObject;
import org.ray.runtime.object.ObjectSerializer;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RaySerializerTest extends BaseMultiLanguageTest {

  @Test
  public void testSerializePyActor() {
    RayPyActor pyActor = Ray.createPyActor("test", "RaySerializerTest");
    WorkerContext workerContext = TestUtils.getRuntime().getWorkerContext();
    NativeRayObject nativeRayObject = ObjectSerializer.serialize(pyActor);
    RayPyActor result = (RayPyActor) ObjectSerializer
        .deserialize(nativeRayObject, null, workerContext.getCurrentClassLoader());
    Assert.assertEquals(result.getId(), pyActor.getId());
    Assert.assertEquals(result.getModuleName(), "test");
    Assert.assertEquals(result.getClassName(), "RaySerializerTest");
  }

}
