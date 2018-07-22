package org.ray.exercise;

import java.util.ArrayList;
import java.util.List;
import org.ray.api.Ray;
import org.ray.api.RayList;
import org.ray.api.RayObject;
import org.ray.api.RayRemote;
import org.ray.core.RayRuntime;

/**
 * Usage of RayList.
 * A list of ``RayObject``, inherited from ``List`` in Java. It can
 * be used as the type for both return value and parameters.
 *
 */
public class Exercise06 {

  /**
   * Main.
   */
  public static void main(String[] args) {
    try {
      Ray.init();
      RayList<Integer> ns = Ray.call_n(Exercise06::sayList, 10, 10);
      for (int i = 0; i < 10; i++) {
        RayObject<Integer> obj = ns.Get(i);
        System.out.println(obj.get());
      }
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
  public static List<Integer> sayList(Integer count) {
    ArrayList<Integer> rets = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      rets.add(i);
    }
    return rets;
  }
}
