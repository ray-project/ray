package org.ray.api.test;

import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.TestUtils;
import org.ray.api.annotation.RayRemote;
import org.ray.api.options.ActorCreationOptions;
import org.testng.Assert;
import org.testng.annotations.Test;

public class WorkerStartingSuffixTest extends BaseTest {

  @RayRemote
  public static class Echo {
    String getSuffix() {
      return System.getProperty("test.suffix");
    }
  }

  @Test
  public void testSuffix() {
    TestUtils.skipTestUnderSingleProcess();
    ActorCreationOptions options = new ActorCreationOptions.Builder()
        .setWorkerStartingSuffix("-Dtest.suffix=suffix")
        .createActorCreationOptions();
    RayActor<Echo> actor = Ray.createActor(Echo::new, options);
    RayObject<String> obj = Ray.call(Echo::getSuffix, actor);
    Assert.assertEquals(obj.get(), "suffix");
  }
}
