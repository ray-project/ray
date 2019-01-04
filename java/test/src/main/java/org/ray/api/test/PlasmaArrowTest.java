package org.ray.api.test;

import org.apache.arrow.plasma.PlasmaClient;
import org.apache.arrow.plasma.exceptions.DuplicateObjectException;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.ray.api.Ray;
import org.ray.api.id.UniqueId;
import org.ray.runtime.AbstractRayRuntime;
import org.ray.runtime.config.RunMode;

public class PlasmaArrowTest extends BaseTest {

  AbstractRayRuntime runtime = (AbstractRayRuntime) Ray.internal();

  @Before
  public void beforeEachCase() {
   // Assume.assumeFalse(Ray.systemInfo.isSingleProcess());
    Assume.assumeFalse(runtime.getRayConfig().runMode == RunMode.SINGLE_PROCESS);
  }

  @Test
  public void test() {
    UniqueId objectId = UniqueId.randomId();
    //when put an object twice in object store ,will print a warn log
    //for example: An object with ID ... already exists in the plasma store.
    try {
      PlasmaClient store = new PlasmaClient(runtime.getRayConfig().objectStoreSocketName, "", 0);
      store.put(objectId.getBytes(), new byte[]{}, "RAW".getBytes());
      store.put(objectId.getBytes(), new byte[]{}, "RAW".getBytes());
      Assert.fail(
              "Fail to throw DuplicateObjectException when put an object into plasma store twice.");
    } catch (DuplicateObjectException e) {
      // An object with this ID already exists in the plasma store.
    }
  }
}
