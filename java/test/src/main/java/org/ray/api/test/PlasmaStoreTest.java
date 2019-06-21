package org.ray.api.test;

import org.ray.api.Ray;
import org.ray.api.TestUtils;
import org.ray.api.id.ObjectId;
import org.ray.runtime.AbstractRayRuntime;
import org.ray.runtime.ObjectInterface;
import org.testng.annotations.Test;

public class PlasmaStoreTest extends BaseTest {

  @Test
  public void testPutWithDuplicateId() {
    TestUtils.skipTestUnderSingleProcess();
    ObjectId objectId = ObjectId.randomId();
    AbstractRayRuntime runtime = (AbstractRayRuntime) Ray.internal();
    ObjectInterface objectInterface = runtime.getWorker().getObjectInterface();
    objectInterface.put(objectId, new byte[] {});
    objectInterface.put(objectId, new byte[] {});
    // Putting 2 objects with duplicate ID should fail but ignored.
  }
}
