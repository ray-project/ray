package org.ray.exercise;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.RayRemote;
import org.ray.api.WaitResult;
import org.ray.core.RayRuntime;

/**
 * Use Ray.wait to ignore stragglers
 */
public class Exercise04 {

  @RayRemote
  public static String f1() {
    String ret = "f1";
    System.out.println(ret);
    return ret;
  }

  @RayRemote
  public static String f2() {
    String ret = "f2";
    System.out.println(ret);
    return ret;
  }

  /**
   * A slow remote function.
   */
  @RayRemote
  public static String f3() {
    String ret = "f3";
    try {
      Thread.sleep(5000L);
    } catch (Exception e) {
      e.printStackTrace();
    }
    System.out.println(ret);
    return ret;
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
      List<RayObject<String>> readyOnes = waitResult.getReadyOnes();
      List<RayObject<String>> remainOnes = waitResult.getRemainOnes();
      System.out.println("Number of readyOnes: " + readyOnes.size());
      for (int i = 0; i < readyOnes.size(); i++) {
        System.out.println("The value of readyOnes " + i + " is " + readyOnes.get(i).get());
      }
      System.out.println("Number of remainOnes: " + remainOnes.size());
      for (int i = 0; i < remainOnes.size(); i++) {
        System.out.println("The value of remainOnes " + i + " is " + remainOnes.get(i).get());
      }
    } catch (Throwable t) {
      t.printStackTrace();
    } finally {
      RayRuntime.getInstance().cleanUp();
    }
  }
}
