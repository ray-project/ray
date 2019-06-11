package org.ray.api.test;

import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.annotation.RayRemote;
import org.ray.api.options.ActorCreationOptions;
import org.testng.annotations.Test;

public class testWorkerStartingSuffix {

  @RayRemote
  public static class Echo {

  }

  @Test
  public void testSuffix() {
    ActorCreationOptions options = new ActorCreationOptions.Builder()
        .setWorkerStartingSuffix("-Xss16m")
        .createActorCreationOptions();
    RayActor<Echo> actor = Ray.createActor(Echo::new, options);

  }
}
