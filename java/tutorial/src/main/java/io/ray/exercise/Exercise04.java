package io.ray.exercise;

import com.google.common.collect.ImmutableList;
import io.ray.api.Ray;
import io.ray.api.RayObject;
import io.ray.api.WaitResult;
import java.util.List;

/**
 * Use Ray.wait to ignore stragglers
 */
public class Exercise04 {

  public static String f1() {
    System.out.println("Executing f1");
    return "f1";
  }

  public static String f2() {
    System.out.println("Executing f2");
    return "f2";
  }

  /**
   * A slow remote function.
   */
  public static String f3() {
    System.out.println("Executing f3");
    try {
      Thread.sleep(5000L);
    } catch (Exception e) {
      e.printStackTrace();
    }
    System.out.println("Finished executing f3");
    return "f3";
  }

  public static void main(String[] args) throws Exception {
    try {
      Ray.init();
      List<RayObject<String>> waitList = ImmutableList.of(
          Ray.call(Exercise04::f1),
          Ray.call(Exercise04::f2),
          Ray.call(Exercise04::f3)
      );
      // Ray.wait will block until specified number of results are ready
      // or specified timeout have passed.
      // In this case, the result of f3 will be ignored.
      WaitResult<String> waitResult = Ray.wait(waitList, 2, 3000);
      System.out.printf("%d ready object(s): \n", waitResult.getReady().size());
      waitResult.getReady().forEach(rayObject -> System.out.println(rayObject.get()));
      System.out.printf("%d unready object(s): \n", waitResult.getUnready().size());
      waitResult.getUnready().forEach(rayObject -> System.out.println(rayObject.getId()));
    } catch (Throwable t) {
      t.printStackTrace();
    } finally {
      Ray.shutdown();
    }
  }
}
