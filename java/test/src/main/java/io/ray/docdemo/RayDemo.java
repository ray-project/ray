package io.ray.docdemo;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This class contains demo code of the Ray introduction doc
 * (https://docs.ray.io/en/master/index.html and
 * https://docs.ray.io/en/master/ray-overview/index.html).
 *
 * <p>Please keep them in sync.
 */
public class RayDemo {

  public static int square(int x) {
    return x * x;
  }

  public static class Counter {

    private int value = 0;

    public void increment() {
      this.value += 1;
    }

    public int read() {
      return this.value;
    }
  }

  public static void main(String[] args) {
    // Initialize Ray runtime.
    Ray.init();
    {
      List<ObjectRef<Integer>> objectRefList = new ArrayList<>();
      // Invoke the `square` method 4 times remotely as Ray tasks.
      // The tasks will run in parallel in the background.
      for (int i = 0; i < 4; i++) {
        objectRefList.add(Ray.task(RayDemo::square, i).remote());
      }
      // Get the actual results of the tasks with `get`.
      System.out.println(Ray.get(objectRefList)); // [0, 1, 4, 9]
    }

    {
      List<ActorHandle<Counter>> counters = new ArrayList<>();
      // Create 4 actors from the `Counter` class.
      // They will run in remote worker processes.
      for (int i = 0; i < 4; i++) {
        counters.add(Ray.actor(Counter::new).remote());
      }

      // Invoke the `increment` method on each actor.
      // This will send an actor task to each remote actor.
      for (ActorHandle<Counter> counter : counters) {
        counter.task(Counter::increment).remote();
      }
      // Invoke the `read` method on each actor, and print the results.
      List<ObjectRef<Integer>> objectRefList =
          counters.stream()
              .map(counter -> counter.task(Counter::read).remote())
              .collect(Collectors.toList());
      System.out.println(Ray.get(objectRefList)); // [1, 1, 1, 1]
    }
  }
}
