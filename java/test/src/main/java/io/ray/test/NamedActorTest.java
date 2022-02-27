package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import java.util.Optional;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class NamedActorTest extends BaseTest {

  public static class Counter {

    private int value = 0;

    public int increment() {
      this.value += 1;
      return this.value;
    }
  }

  @BeforeClass
  void initNamespace() {
    System.setProperty("ray.job.namespace", "named_actor_test");
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

  @Test
  public void testGetNonExistingNamedActor() {
    Assert.assertTrue(!Ray.getActor("non_existing_actor").isPresent());
  }
}
