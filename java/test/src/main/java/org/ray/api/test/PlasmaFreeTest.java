package org.ray.api.test;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.WaitResult;
import org.ray.api.annotation.RayRemote;
import org.ray.api.id.UniqueId;
import org.ray.runtime.RayNativeRuntime;
import org.ray.runtime.util.UniqueIdUtil;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PlasmaFreeTest extends BaseTest {

  @RayRemote
  private static String hello() {
    return "hello";
  }

  @Test
  public void test() {
    RayObject<String> helloId = Ray.call(PlasmaFreeTest::hello);
    String helloString = helloId.get();
    Assert.assertEquals("hello", helloString);
    List<RayObject<String>> waitFor = ImmutableList.of(helloId);
    WaitResult<String> waitResult = Ray.wait(waitFor, 1, 2 * 1000);
    List<RayObject<String>> readyOnes = waitResult.getReady();
    List<RayObject<String>> unreadyOnes = waitResult.getUnready();
    Assert.assertEquals(1, readyOnes.size());
    Assert.assertEquals(0, unreadyOnes.size());

    List<UniqueId> freeList = new ArrayList<>();
    freeList.add(helloId.getId());
    Ray.internal().free(freeList, true, false);
    // Flush: trigger the release function because Plasma Client has cache.
    for (int i = 0; i < 128; i++) {
      Ray.call(PlasmaFreeTest::hello).get();
    }

    // Check if the object has been evicted. Don't give ray.wait enough
    // time to reconstruct the object.
    waitResult = Ray.wait(waitFor, 1, 0);
    readyOnes = waitResult.getReady();
    unreadyOnes = waitResult.getUnready();
    Assert.assertEquals(0, readyOnes.size());
    Assert.assertEquals(1, unreadyOnes.size());

    testDeleteCreatingTasks(true);
    testDeleteCreatingTasks(false);
  }

  private void testDeleteCreatingTasks(boolean deleteCreatingTasks) {
    RayObject<String> helloId = Ray.call(PlasmaFreeTest::hello);
    String helloString = helloId.get();
    Assert.assertEquals("hello", helloString);

    Ray.internal().free(ImmutableList.of(helloId.getId()), true, deleteCreatingTasks);

    final boolean taskExists = ((RayNativeRuntime) Ray.internal())
        .rayletTaskExistsInGcs(UniqueIdUtil.computeTaskId(helloId.getId()));

    // If deleteCreatingTasks, the creating task should not be in GCS.
    Assert.assertEquals(deleteCreatingTasks, !taskExists);
  }

}
