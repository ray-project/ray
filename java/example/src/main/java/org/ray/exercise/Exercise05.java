package org.ray.exercise;

import org.ray.api.Ray;
import org.ray.api.RayRemote;
import org.ray.api.returns.MultipleReturns2;
import org.ray.api.returns.RayObjects2;
import org.ray.core.RayRuntime;

/**
 * Enable multiple heterogeneous return values
 * Java worker support at most four multiple heterogeneous return values,
 * and in order to let the runtime know the number of return values we
 * supply the method of ``Ray.call_X`` as follows.
 */
public class Exercise05 {

  /**
   * Main.
   */
  public static void main(String[] args) {
    try {
      Ray.init();
      RayObjects2<Integer, String> refs = Ray.call_2(Exercise05::sayMultiRet);
      Integer obj1 = refs.r0().get();
      String obj2 = refs.r1().get();
      System.out.println(obj1);
      System.out.println(obj2);
    } catch (Throwable t) {
      t.printStackTrace();
    } finally {
      RayRuntime.getInstance().cleanUp();
    }
  }

  /**
   * Remote function with MultipleReturns.
   */
  @RayRemote
  public static MultipleReturns2<Integer, String> sayMultiRet() {
    return new MultipleReturns2<Integer, String>(123, "123");
  }
}
