package io.ray.docdemo;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.docdemo.WalkthroughDemo.Counter;
import java.util.Optional;
import org.testng.Assert;

public class UsingActorsDemo {

  // A regular Java class.
  public static class Counter {

    private int value = 0;

    public int increment() {
      this.value += 1;
      return this.value;
    }
  }

  public static class CounterFactory {

    public static Counter createCounter() {
      return new Counter();
    }
  }

  public static class Foo {

    public int bar() {
      return 1;
    }
  }

  public static class GpuActor {

  }

  public static void main(String[] args) {
    Ray.init();

    {
      // Create an actor with a constructor.
      Ray.actor(Counter::new).remote();
      // Create an actor with a factory method.
      Ray.actor(CounterFactory::createCounter).remote();
    }

    {
      ActorHandle<Foo> foo = Ray.actor(Foo::new).remote();
      Assert.assertEquals((int) foo.task(Foo::bar).remote().get(), 1);
    }

    {
      Ray.actor(GpuActor::new).setResource("CPU", 2.0).setResource("GPU", 0.5).remote();
    }

    {
      Ray.actor(GpuActor::new).setResource("Resource2", 1.0).remote();
    }

    {
      ActorHandle<Counter> a1 = Ray.actor(Counter::new).setResource("CPU", 1.0)
        .setResource("Custom1", 1.0).remote();
      ActorHandle<Counter> a2 = Ray.actor(Counter::new).setResource("CPU", 2.0)
        .setResource("Custom2", 1.0).remote();
      ActorHandle<Counter> a3 = Ray.actor(Counter::new).setResource("CPU", 3.0)
        .setResource("Custom3", 1.0).remote();
    }

    {
      ActorHandle<Foo> actorHandle = Ray.actor(Foo::new).remote();
      actorHandle.kill(/*noRestart=*/true);
    }

    {
      // Create an actor with a name
      ActorHandle<Counter> counter = Ray.actor(Counter::new).setGlobalName("some_name").remote();
    }
    {
      // Retrieve the actor later
      Optional<ActorHandle<Counter>> counter = Ray.getGlobalActor("some_name");
      Assert.assertTrue(counter.isPresent());
    }
  }
}
