package org.ray.api.test;

import com.google.common.collect.ImmutableList;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.TestUtils;
import org.ray.api.annotation.RayRemote;
import org.ray.api.exception.RayActorException;
import org.ray.api.id.ObjectId;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = {"directCall"})
public class KillActorTest extends BaseTest {

  @RayRemote
  public static class HangActor {

    public boolean hang() {
      // Never returns.
      Ray.get(ObjectId.fromRandom());
      return true;
    }
  }

  public void testKillActor() {
    TestUtils.skipTestUnderSingleProcess();
    TestUtils.skipTestIfDirectActorCallEnabled(false);
    RayActor<HangActor> actor = Ray.createActor(HangActor::new);
    RayObject<Boolean> result = Ray.call(HangActor::hang, actor);
    Assert.assertEquals(0, Ray.wait(ImmutableList.of(result), 1, 100).getReady().size());
    Ray.killActor(actor);
    Assert.assertEquals(1, Ray.wait(ImmutableList.of(result), 1, 1000).getReady().size());
    Assert.expectThrows(RayActorException.class, result::get);
  }
}
