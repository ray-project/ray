package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.api.options.ActorLifetime;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
public class ActorLifetimeTest {

  private static class MyActor {
    public String echo(String str) {
      return str;
    }
  }

  public void testDetached() throws IOException, InterruptedException {
    System.setProperty("ray.job.namespace", "test2");
    Ray.init();
    startDriver(DriverClassWithDetachedActor.class);
    Assert.assertTrue(Ray.getActor("my_actor").isPresent());
    Ray.shutdown();
  }

  public void testNonDetached() throws IOException, InterruptedException {
    System.setProperty("ray.job.namespace", "test2");
    Ray.init();
    startDriver(DriverClassWithNonDetachedActor.class);
    Assert.assertFalse(Ray.getActor("my_actor").isPresent());
    Ray.shutdown();
  }

  private static class DriverClassWithDetachedActor {
    public static void main(String[] argv) {
      System.setProperty("ray.job.long-running", "true");
      System.setProperty("ray.job.namespace", "test2");
      Ray.init();
      ActorHandle<MyActor> myActor =
          Ray.actor(MyActor::new).setLifetime(ActorLifetime.DETACHED).setName("my_actor").remote();
      myActor.task(MyActor::echo, "hello").remote().get();
      Ray.shutdown();
    }
  }

  private static class DriverClassWithNonDetachedActor {
    public static void main(String[] argv) {
      System.setProperty("ray.job.namespace", "test2");
      Ray.init();
      ActorHandle<MyActor> myActor = Ray.actor(MyActor::new).setName("my_actor").remote();
      myActor.task(MyActor::echo, "hello").remote().get();
      Ray.shutdown();
    }
  }

  private static void startDriver(Class<?> driverClass) throws IOException, InterruptedException {
    Process driver = null;
    try {
      ProcessBuilder builder = TestUtils.buildDriver(driverClass, null);
      builder.redirectError(ProcessBuilder.Redirect.INHERIT);
      driver = builder.start();
      // Wait for driver to start.
      driver.waitFor();
      System.out.println("Driver finished.");
    } finally {
      if (driver != null) {
        driver.waitFor(1, TimeUnit.SECONDS);
      }
    }
  }
}
