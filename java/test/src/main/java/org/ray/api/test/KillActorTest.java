package org.ray.api.test;

import com.google.common.collect.ImmutableList;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.TestUtils;
import org.ray.api.annotation.RayRemote;
import org.ray.api.exception.RayActorException;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class KillActorTest extends BaseTest {

  @RayRemote
  public static class HangActor {

    public boolean alive() {
      return true;
    }

    public boolean hang() throws InterruptedException {
      while (true) {
        Thread.sleep(1000);
      }
    }
  }

  public void testKillActor() {
    TestUtils.skipTestUnderSingleProcess();
    TestUtils.skipTestIfDirectActorCallDisabled();
    RayActor<HangActor> actor = Ray.createActor(HangActor::new);
    Assert.assertTrue(Ray.call(HangActor::alive, actor).get());
    RayObject<Boolean> result = Ray.call(HangActor::hang, actor);
    Assert.assertEquals(0, Ray.wait(ImmutableList.of(result), 1, 500).getReady().size());
    Ray.killActor(actor);
    Assert.expectThrows(RayActorException.class, result::get);
  }
}
