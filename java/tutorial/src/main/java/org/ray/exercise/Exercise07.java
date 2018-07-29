package org.ray.exercise;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.ray.api.Ray;
import org.ray.api.RayMap;
import org.ray.api.RayObject;
import org.ray.api.RayRemote;
import org.ray.core.RayRuntime;

/**
 * Show usage of RayMap.
 * {@code RayMap} is a map of {@code RayObject}s, inherited from {@code Map}.
 * It can be used as the type for both return values and parameters.
 */
public class Exercise07 {

  public static void main(String[] args) {
    try {
      Ray.init();
      RayMap<Integer, String> ns = Ray.call_n(Exercise07::sayMap,
          Arrays.asList(1, 2, 4, 3), "n_futures_");
      for (Map.Entry<Integer, RayObject<String>> ne : ns.EntrySet()) {
        Integer key = ne.getKey();
        RayObject<String> obj = ne.getValue();
        System.out.println(obj.get());
      }
    } catch (Throwable t) {
      t.printStackTrace();
    } finally {
      RayRuntime.getInstance().cleanUp();
    }
  }

  /**
   * A remote function that returns a map.
   */
  @RayRemote()
  public static Map<Integer, String> sayMap(Collection<Integer> ids, String prefix) {
    Map<Integer, String> ret = new HashMap<>();
    for (int id : ids) {
      ret.put(id, prefix + id);
    }
    return ret;
  }
}
