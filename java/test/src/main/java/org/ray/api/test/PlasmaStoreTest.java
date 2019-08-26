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
    objectStore.put("1", objectId);
    Assert.assertEquals(Ray.get(objectId), "1");
    objectStore.put("2", objectId);
    // Putting the second object with duplicate ID should fail but ignored.
    Assert.assertEquals(Ray.get(objectId), "1");
  }
}
