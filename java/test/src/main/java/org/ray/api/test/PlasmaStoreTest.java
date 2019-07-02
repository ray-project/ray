package org.ray.api.test;

import org.apache.arrow.plasma.PlasmaClient;
import org.apache.arrow.plasma.exceptions.DuplicateObjectException;

import org.ray.api.Ray;
import org.ray.api.TestUtils;
import org.ray.api.id.UniqueId;
import org.ray.runtime.AbstractRayRuntime;
import org.testng.Assert;
import org.testng.annotations.Test;

public class PlasmaStoreTest extends BaseTest {

  @Test
  public void testPutWithDuplicateId() {
    TestUtils.skipTestUnderSingleProcess();
    UniqueId objectId = UniqueId.randomId();
    AbstractRayRuntime runtime = (AbstractRayRuntime) Ray.internal();
    PlasmaClient store = new PlasmaClient(runtime.getRayConfig().objectStoreSocketName, "", 0);
    store.put(objectId.getBytes(), new byte[]{}, new byte[]{});
    try {
      store.put(objectId.getBytes(), new byte[]{}, new byte[]{});
      Assert.fail("This line shouldn't be reached.");
    } catch (DuplicateObjectException e) {
      // Putting 2 objects with duplicate ID should throw DuplicateObjectException.
    }
  }
}
