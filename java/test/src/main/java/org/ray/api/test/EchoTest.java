package org.ray.api.test;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.ray.api.Ray;
import org.ray.api.RayRemote;

@RunWith(MyRunner.class)
public class EchoTest {

  @Test
  public void test() {
    long startTime, endTime;
    for (int i = 0; i < 100; i++) {
      startTime = System.nanoTime();
      String ret = echo("Ray++" + i);
      endTime = System.nanoTime();
      System.out.println("echo: " + ret + " , total time is " + (endTime - startTime));
    }
  }


  public String echo(String who) {

    return Ray.call(
        EchoTest::recho,
        Ray.call(EchoTest::Hi),
        Ray.call(EchoTest::Who, who)
    ).get();
  }

  @RayRemote
  public static String Hi() {
    return "Hi";
  }

  @RayRemote
  public static String Who(String who) {
    return who;
  }

  @RayRemote
  public static String recho(String pre, String who) {
    return pre + ", " + who + "!";
  }
}
