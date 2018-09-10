package org.ray.api.test;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.WaitResult;
import org.ray.api.annotation.RayRemote;
import org.ray.api.id.UniqueId;
import org.ray.core.AbstractRayRuntime;


@RunWith(MyRunner.class)
public class PlasmaFreeTest {

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
    Ray.internal().free(freeList, true);
    // Flush: trigger the release function because Plasma Client has cache.
    for (int i = 0; i < 128; i++) {
      Ray.call(PlasmaFreeTest::hello).get();
    }

    waitResult = Ray.wait(waitFor, 1, 2 * 1000);
    readyOnes = waitResult.getReady();
    unreadyOnes = waitResult.getUnready();
    Assert.assertEquals(0, readyOnes.size());
    Assert.assertEquals(1, unreadyOnes.size());
  }
}
