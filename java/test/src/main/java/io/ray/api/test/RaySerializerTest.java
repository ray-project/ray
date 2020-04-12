package io.ray.api.test;

import io.ray.api.Ray;
import io.ray.api.RayPyActor;
import io.ray.api.function.PyActorClass;
import io.ray.runtime.object.NativeRayObject;
import io.ray.runtime.object.ObjectSerializer;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RaySerializerTest extends BaseMultiLanguageTest {

  @Test
  public void testSerializePyActor() {
    RayPyActor pyActor = Ray.createActor(new PyActorClass("test", "RaySerializerTest"));
    NativeRayObject nativeRayObject = ObjectSerializer.serialize(pyActor);
    RayPyActor result = (RayPyActor) ObjectSerializer
        .deserialize(nativeRayObject, null, Object.class);
    Assert.assertEquals(result.getId(), pyActor.getId());
    Assert.assertEquals(result.getModuleName(), "test");
    Assert.assertEquals(result.getClassName(), "RaySerializerTest");
  }

}
