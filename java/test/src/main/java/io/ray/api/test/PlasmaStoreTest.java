package io.ray.api.test;

import io.ray.api.Ray;
import io.ray.api.TestUtils;
import io.ray.api.id.ObjectId;
import io.ray.runtime.object.ObjectStore;
import org.testng.Assert;
import org.testng.annotations.Test;

public class PlasmaStoreTest extends BaseTest {

  @Test
  public void testPutWithDuplicateId() {
    TestUtils.skipTestUnderSingleProcess();
    ObjectId objectId = ObjectId.fromRandom();
    ObjectStore objectStore = TestUtils.getRuntime().getObjectStore();
    objectStore.put("1", objectId);
    Assert.assertEquals(Ray.get(objectId, String.class), "1");
    objectStore.put("2", objectId);
    // Putting the second object with duplicate ID should fail but ignored.
    Assert.assertEquals(Ray.get(objectId, String.class), "1");
  }
}
