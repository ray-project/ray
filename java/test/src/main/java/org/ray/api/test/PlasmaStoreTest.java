package org.ray.api.test;

import org.ray.api.Ray;
import org.ray.api.TestUtils;
import org.ray.api.id.ObjectId;
import org.ray.runtime.AbstractRayRuntime;
import org.ray.runtime.objectstore.ObjectStoreProxy;
import org.testng.Assert;
import org.testng.annotations.Test;

public class PlasmaStoreTest extends BaseTest {

  @Test
  public void testPutWithDuplicateId() {
    TestUtils.skipTestUnderSingleProcess();
    ObjectId objectId = ObjectId.fromRandom();
    AbstractRayRuntime runtime = (AbstractRayRuntime) Ray.internal();
    ObjectStoreProxy objectInterface = runtime.getObjectStoreProxy();
    objectInterface.put(objectId, 1);
    Assert.assertEquals(objectInterface.<Integer>get(objectId), (Integer) 1);
    objectInterface.put(objectId, 2);
    // Putting 2 objects with duplicate ID should fail but ignored.
    Assert.assertEquals(objectInterface.<Integer>get(objectId), (Integer) 1);
  }
}
