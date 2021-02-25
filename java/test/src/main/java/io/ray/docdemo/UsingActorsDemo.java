package io.ray.docdemo;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.api.function.RayFunc1;
import io.ray.api.function.RayFunc2;
import io.ray.api.function.RayFunc3;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;

/**
 * This class contains demo code of the Ray core Using Actors doc
 * (https://docs.ray.io/en/master/actors.html).
 *
 * <p>Please keep them in sync.
 */
public class UsingActorsDemo {

  // A regular Java class.
  public static class Counter {

    private int value = 0;

    public int increment() {
      this.value += 1;
      return this.value;
    }

    public int getCounter() {
      return this.value;
    }

    public void reset(int newValue) {
      this.value = newValue;
    }
  }

  public static class CounterOverloaded extends Counter {
    public int increment(int diff) {
      super.value += diff;
      return super.value;
    }

    public int increment(int diff1, int diff2) {
      super.value += diff1 + diff2;
      return super.value;
    }
  }

  public static class CounterFactory {

    public static Counter createCounter() {
      return new Counter();
    }
  }

  public static class GpuActor {}

  public static class MyRayApp {

    public static void foo(ActorHandle<Counter> counter) throws InterruptedException {
      for (int i = 0; i < 1000; i++) {
        TimeUnit.MILLISECONDS.sleep(100);
        counter.task(Counter::increment).remote();
      }
    }
  }

  public static void main(String[] args) throws InterruptedException {
    Ray.init();

    {
      // Create an actor with a constructor.
      Ray.actor(Counter::new).remote();
      // Create an actor with a factory method.
      Ray.actor(CounterFactory::createCounter).remote();
    }

    {
      ActorHandle<Counter> a = Ray.actor(Counter::new).remote();
      // Call an actor method with a return value
      Assert.assertEquals((int) a.task(Counter::increment).remote().get(), 1);
      // Call an actor method without return value
      a.task(Counter::reset, 10).remote();
      Assert.assertEquals((int) a.task(Counter::increment).remote().get(), 11);
    }

    {
      ActorHandle<CounterOverloaded> a = Ray.actor(CounterOverloaded::new).remote();
      // Call an overloaded actor method by super class method reference.
      Assert.assertEquals((int) a.task(Counter::increment).remote().get(), 1);
      // Call an overloaded actor method, cast method reference first.
      a.task((RayFunc1<CounterOverloaded, Integer>) CounterOverloaded::increment).remote();
      a.task((RayFunc2<CounterOverloaded, Integer, Integer>) CounterOverloaded::increment, 10)
          .remote();
      RayFunc3<CounterOverloaded, Integer, Integer, Integer> f = CounterOverloaded::increment;
      a.task(f, 10, 10).remote();
      Assert.assertEquals((int) a.task(Counter::increment).remote().get(), 33);
    }

    {
      Ray.actor(GpuActor::new).setResource("CPU", 2.0).setResource("GPU", 0.5).remote();
    }

    {
      Ray.actor(GpuActor::new).setResource("Resource2", 1.0).remote();
    }

    {
      ActorHandle<Counter> a1 =
          Ray.actor(Counter::new).setResource("CPU", 1.0).setResource("Custom1", 1.0).remote();
      ActorHandle<Counter> a2 =
          Ray.actor(Counter::new).setResource("CPU", 2.0).setResource("Custom2", 1.0).remote();
      ActorHandle<Counter> a3 =
          Ray.actor(Counter::new).setResource("CPU", 3.0).setResource("Custom3", 1.0).remote();
    }

    {
      ActorHandle<Counter> actorHandle = Ray.actor(Counter::new).remote();
      actorHandle.kill();
    }

    {
      // Create an actor with a globally unique name
      ActorHandle<Counter> counter = Ray.actor(Counter::new).setGlobalName("some_name").remote();
    }
    {
      // Retrieve the actor later somewhere
      Optional<ActorHandle<Counter>> counter = Ray.getGlobalActor("some_name");
      Assert.assertTrue(counter.isPresent());
    }

    {
      // Create an actor with a job-scope-unique name
      ActorHandle<Counter> counter = Ray.actor(Counter::new).setName("some_name_in_job").remote();
    }
    {
      // Retrieve the actor later somewhere in the same job
      Optional<ActorHandle<Counter>> counter = Ray.getActor("some_name_in_job");
      Assert.assertTrue(counter.isPresent());
    }

    {
      ActorHandle<Counter> counter = Ray.actor(Counter::new).remote();

      // Start some tasks that use the actor.
      for (int i = 0; i < 3; i++) {
        Ray.task(MyRayApp::foo, counter).remote();
      }

      // Print the counter value.
      for (int i = 0; i < 10; i++) {
        TimeUnit.SECONDS.sleep(1);
        System.out.println(counter.task(Counter::getCounter).remote().get());
      }
    }
  }
}
