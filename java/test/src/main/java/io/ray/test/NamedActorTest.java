package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import java.util.Optional;
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
    Assert.assertEquals(namedActor.get().task(Counter::increment).remote().get(),
        Integer.valueOf(2));
  }

  @Test
  public void testGlobalActor() {
    String name = "global-actor-counter";
    // Create an actor.
    ActorHandle<Counter> actor = Ray.actor(Counter::new).setName(name, true).remote();
    Assert.assertEquals(actor.task(Counter::increment).remote().get(), Integer.valueOf(1));
    Assert.assertFalse(Ray.getActor(name).isPresent());
    // Get the named actor.
    Optional<ActorHandle<Counter>> namedActor = Ray.getActor(name, true);
    Assert.assertTrue(namedActor.isPresent());
    // Verify that this handle is correct.
    Assert.assertEquals(namedActor.get().task(Counter::increment).remote().get(),
        Integer.valueOf(2));
  }

  public void testActorDuplicatedName() {
    String name = "named-actor-counter";
    // Create an actor.
    ActorHandle<Counter> actor = Ray.actor(Counter::new).setName(name).remote();
    // Registering with the same name should fail.
    Ray.actor(Counter::new).setName(name).remote();
  }

}
