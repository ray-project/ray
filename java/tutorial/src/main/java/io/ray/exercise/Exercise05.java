package io.ray.exercise;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;

/**
 * Show usage of actors.
 */
public class Exercise05 {

  public static void main(String[] args) {
    try {
      Ray.init();
      // `Ray.createActor` creates an actor instance.
      ActorHandle<Adder> adder = Ray.createActor(Adder::new, 0);
      // Use `Ray.call(actor, parameters)` to call an actor method.
      ObjectRef<Integer> result1 = adder.call(Adder::add, 1);
      System.out.println(result1.get());
      ObjectRef<Integer> result2 = adder.call(Adder::add, 10);
      System.out.println(result2.get());
    } catch (Throwable t) {
      t.printStackTrace();
    } finally {
      Ray.shutdown();
    }
  }

  /**
   * An example actor.
   */
  public static class Adder {

    public Adder(int initValue) {
      sum = initValue;
    }

    public int add(int n) {
      return sum += n;
    }

    private int sum;
  }
}
