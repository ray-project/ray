package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.runtime.config.RayConfig;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;

public class NamedActorTest extends BaseTest {

  public static class Counter {

    private int value = 0;

    public int increment() {
      this.value += 1;
      return this.value;
    }
  }

  @Test
  public void testNamedActor() {
    String name = "named-actor-counter";
    // Create an actor.
    ActorHandle<Counter> actor = Ray.actor(Counter::new).setName(name).remote();
    Assert.assertEquals(actor.task(Counter::increment).remote().get(), Integer.valueOf(1));
    // Get the named actor.
    Assert.assertTrue(Ray.getActor(name).isPresent());
    Optional<ActorHandle<Counter>> namedActor = Ray.getActor(name);
    Assert.assertTrue(namedActor.isPresent());
    // Verify that this handle is correct.
    Assert.assertEquals(
        namedActor.get().task(Counter::increment).remote().get(), Integer.valueOf(2));
  }

  @Test
  public void testGlobalActor() throws IOException, InterruptedException {
    String name = "global-actor-counter";
    // Create an actor.
    ActorHandle<Counter> actor = Ray.actor(Counter::new).setGlobalName(name).remote();
    Assert.assertEquals(actor.task(Counter::increment).remote().get(), Integer.valueOf(1));

    Assert.assertFalse(Ray.getActor(name).isPresent());

    // Get the global actor.
    Optional<ActorHandle<Counter>> namedActor = Ray.getGlobalActor(name);
    Assert.assertTrue(namedActor.isPresent());
    // Verify that this handle is correct.
    Assert.assertEquals(
        namedActor.get().task(Counter::increment).remote().get(), Integer.valueOf(2));

    if (!TestUtils.isSingleProcessMode()) {
      // Get the global actor from another driver.
      RayConfig rayConfig = TestUtils.getRuntime().getRayConfig();
      ProcessBuilder builder =
          new ProcessBuilder(
              "java",
              "-cp",
              System.getProperty("java.class.path"),
              "-Dray.address=" + rayConfig.getRedisAddress(),
              "-Dray.object-store.socket-name=" + rayConfig.objectStoreSocketName,
              "-Dray.raylet.socket-name=" + rayConfig.rayletSocketName,
              "-Dray.raylet.node-manager-port=" + rayConfig.getNodeManagerPort(),
              NamedActorTest.class.getName(),
              name);
      builder.redirectError(ProcessBuilder.Redirect.INHERIT);
      Process driver = builder.start();
      Assert.assertTrue(driver.waitFor(60, TimeUnit.SECONDS));
      Assert.assertEquals(
          driver.exitValue(), 0, "The driver exited with code " + driver.exitValue());

      Assert.assertEquals(
          namedActor.get().task(Counter::increment).remote().get(), Integer.valueOf(4));
    }
  }

  public static void main(String[] args) {
    Ray.init();
    String actorName = args[0];
    // Get the global actor.
    Optional<ActorHandle<Counter>> namedActor = Ray.getGlobalActor(actorName);
    Assert.assertTrue(namedActor.isPresent());
    // Verify that this handle is correct.
    Assert.assertEquals(
        namedActor.get().task(Counter::increment).remote().get(), Integer.valueOf(3));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testActorDuplicatedName() {
    String name = "named-actor-counter";
    // Create an actor.
    ActorHandle<Counter> actor = Ray.actor(Counter::new).setName(name).remote();
    // Ensure async actor creation is finished.
    Assert.assertEquals(actor.task(Counter::increment).remote().get(), Integer.valueOf(1));
    // Registering with the same name should fail.
    Ray.actor(Counter::new).setName(name).remote();
  }
}
