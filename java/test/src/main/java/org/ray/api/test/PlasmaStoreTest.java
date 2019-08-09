package org.ray.api.test;

import java.util.Collections;
import org.ray.api.Ray;
import org.ray.api.TestUtils;
import org.ray.api.id.ObjectId;
import org.ray.runtime.AbstractRayRuntime;
import org.ray.runtime.object.NativeRayObject;
import org.ray.runtime.object.ObjectStore;
import org.testng.Assert;
import org.testng.annotations.Test;

public class PlasmaStoreTest extends BaseTest {

  @Test
  public void testPutWithDuplicateId() {
    TestUtils.skipTestUnderSingleProcess();
    ObjectId objectId = ObjectId.fromRandom();
    AbstractRayRuntime runtime = (AbstractRayRuntime) Ray.internal();
    ObjectStore objectStore = runtime.getObjectStore();
    objectStore.putRaw(new NativeRayObject(new byte[]{1}, null), objectId);
    Assert.assertEquals(
        objectStore.getRaw(Collections.singletonList(objectId), -1).get(0).data[0],
        (byte) 1);
    objectStore.putRaw(new NativeRayObject(new byte[]{2}, null), objectId);
    // Putting 2 objects with duplicate ID should fail but ignored.
    Assert.assertEquals(
        objectStore.getRaw(Collections.singletonList(objectId), -1).get(0).data[0],
        (byte) 1);
  }
}
