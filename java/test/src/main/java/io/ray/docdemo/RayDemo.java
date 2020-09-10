package io.ray.docdemo;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This class contains demo code of the Ray introduction doc
 * (https://docs.ray.io/en/master/index.html and  https://docs.ray.io/en/master/ray-overview/index.html).
 *
 * Please keep them in sync.
 */
public class RayDemo {

  public static int f(int x) {
    return x * x;
  }

  public static class Counter {

    private int n = 0;

    public void increment() {
      this.n += 1;
    }

    public int read() {
      return this.n;
    }
  }

  public static void main(String[] args) {
    Ray.init();
    {
      List<ObjectRef<Integer>> objectRefList = new ArrayList<>();
      for (int i = 0; i < 4; i++) {
        objectRefList.add(Ray.task(RayDemo::f, i).remote());
      }
      System.out.println(Ray.get(objectRefList));  // [0, 1, 4, 9]
    }

    {
      List<ActorHandle<Counter>> counters = new ArrayList<>();
      for (int i = 0; i < 4; i++) {
        counters.add(Ray.actor(Counter::new).remote());
      }

      for (ActorHandle<Counter> counter : counters) {
        counter.task(Counter::increment).remote();
      }
      List<ObjectRef<Integer>> objectRefList = counters.stream()
          .map(counter -> counter.task(Counter::read).remote())
          .collect(Collectors.toList());
      System.out.println(Ray.get(objectRefList));  // [1, 1, 1, 1]
    }
  }
}
