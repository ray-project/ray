package org.ray.api.test;

import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.annotation.RayRemote;
import org.ray.api.options.ActorCreationOptions;
import org.testng.Assert;
import org.testng.annotations.Test;

public class testWorkerStartingSuffix extends BaseTest {

  @RayRemote
  public static class Echo {
    String sayHi() {
      return "hi";
    }

    String getSuffix() {
      return System.getProperty("test.suffix");
    }
  }

  @Test
  public void testSuffix() {
    ActorCreationOptions options = new ActorCreationOptions.Builder()
        .setWorkerStartingSuffix("-Dtest.suffix=suffix")
        .createActorCreationOptions();
    RayActor<Echo> actor = Ray.createActor(Echo::new, options);
    RayObject<String> obj = Ray.call(Echo::getSuffix, actor);
    Assert.assertEquals(obj.get(), "suffix");

    // test without suffix
    RayActor<Echo> actor1 = Ray.createActor(Echo::new, options);
    RayObject<String> obj1 = Ray.call(Echo::sayHi, actor);
    Assert.assertEquals(obj1.get(), "hi");
  }
}
