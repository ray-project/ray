package io.ray.test;

import io.ray.api.ObjectRef;
import io.ray.api.PyActorHandle;
import io.ray.api.Ray;
import io.ray.api.function.PyActorClass;
import io.ray.runtime.generated.RuntimeEnvCommon;
import io.ray.runtime.object.NativeRayObject;
import io.ray.runtime.object.ObjectSerializer;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
public class RaySerializerTest extends BaseTest {

  @Test
  public void testSerializePyActor() {
    PyActorHandle pyActor = Ray.actor(PyActorClass.of("test", "RaySerializerTest")).remote();
    NativeRayObject nativeRayObject = ObjectSerializer.serialize(pyActor);
    PyActorHandle result =
        (PyActorHandle) ObjectSerializer.deserialize(nativeRayObject, null, Object.class);
    Assert.assertEquals(result.getId(), pyActor.getId());
    Assert.assertEquals(result.getModuleName(), "test");
    Assert.assertEquals(result.getClassName(), "RaySerializerTest");
  }

  @Test
  public void testSerializeProtobuf() {
    RuntimeEnvCommon.RuntimeEnv env =
        RuntimeEnvCommon.RuntimeEnv.newBuilder().setWorkingDir("working_dir").build();
    ObjectRef<RuntimeEnvCommon.RuntimeEnv> ref = Ray.put(env);
    RuntimeEnvCommon.RuntimeEnv newEnv = ref.get();
    Assert.assertEquals(env.getWorkingDir(), newEnv.getWorkingDir());
  }
}
