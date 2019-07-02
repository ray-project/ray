package org.ray.api.test;

import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.TestUtils;
import org.ray.api.annotation.RayRemote;
import org.ray.api.options.ActorCreationOptions;
import org.testng.Assert;
import org.testng.annotations.Test;

public class WorkerJvmOptionsTest extends BaseTest {

  @RayRemote
  public static class Echo {
    String getOptions() {
      return System.getProperty("test.suffix");
    }
  }

  @Test
  public void testJvmOptions() {
    TestUtils.skipTestUnderSingleProcess();
    ActorCreationOptions options = new ActorCreationOptions.Builder()
        .setJvmOptions("-Dtest.suffix=suffix")
        .createActorCreationOptions();
    RayActor<Echo> actor = Ray.createActor(Echo::new, options);
    RayObject<String> obj = Ray.call(Echo::getOptions, actor);
    Assert.assertEquals(obj.get(), "suffix");
  }
}
