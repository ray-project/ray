package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.options.ActorCreationOptions;
import org.testng.Assert;
import org.testng.annotations.Test;

public class WorkerJvmOptionsTest extends BaseTest {

  public static class Echo {
    String getOptions() {
      return System.getProperty("test.suffix");
    }
  }

  @Test
  public void testJvmOptions() {
    TestUtils.skipTestUnderSingleProcess();
    ActorCreationOptions options = new ActorCreationOptions.Builder()
        // The whitespaces in following argument are intentionally added to test
        // that raylet can correctly handle dynamic options with whitespaces.
        .setJvmOptions(" -Dtest.suffix=suffix -Dtest.suffix1=suffix1 ")
        .createActorCreationOptions();
    ActorHandle<Echo> actor = Ray.createActor(Echo::new, options);
    ObjectRef<String> obj = actor.call(Echo::getOptions);
    Assert.assertEquals(obj.get(), "suffix");
  }
}
