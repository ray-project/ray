package org.ray.exercise;

import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.annotation.RayRemote;

/**
 * Show usage of actors.
 */
public class Exercise05 {

  public static void main(String[] args) {
    try {
      Ray.init();
      // `Ray.createActor` creates an actor instance.
      RayActor<Adder> adder = Ray.createActor(Adder.class);
      // Use `Ray.call(actor, parameters)` to call an actor method.
      RayObject<Integer> result1 = Ray.call(Adder::add, adder, 1);
      System.out.println(result1.get());
      RayObject<Integer> result2 = Ray.call(Adder::add, adder, 10);
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
  // `@RayRemote` annotation also converts a normal class to an actor.
  @RayRemote
  public static class Adder {

    public Adder() {
      sum = 0;
    }

    public int add(int n) {
      return sum += n;
    }

    private int sum;
  }
}
