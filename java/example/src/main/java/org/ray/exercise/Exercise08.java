package org.ray.exercise;

import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.RayRemote;
import org.ray.core.RayRuntime;

/**
 * Actor Support of create Actor and call Actor method.
 */
public class Exercise08 {

  /**
   * Main.
   */
  public static void main(String[] args) {
    try {
      Ray.init();
      RayActor<Adder> adder = Ray.create(Adder.class);
      RayObject<Integer> result1 = Ray.call(Adder::add, adder, 1);
      RayObject<Integer> result2 = Ray.call(Adder::add, adder, 10);
      System.out.println(result1.get());
      System.out.println(result2.get());
    } catch (Throwable t) {
      t.printStackTrace();
    } finally {
      RayRuntime.getInstance().cleanUp();
    }
  }

  /**
   * Remote function.
   */
  @RayRemote
  public static class Adder {
    public Adder() {
      sum = 0;
    }

    public Integer add(Integer n) {
      return sum += n;
    }

    private Integer sum;
  }
}
