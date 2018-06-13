package org.ray.api.test;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.ray.api.Ray;
import org.ray.api.RayRemote;

@RunWith(MyRunner.class)
public class EchoTest {

  @RayRemote
  public static String hi() {
    return "hi";
  }

  @RayRemote
  public static String who(String who) {
    return who;
  }

  @RayRemote
  public static String recho(String pre, String who) {
    return pre + ", " + who + "!";
  }

  @Test
  public void test() {
    long startTime = 0;
    long endTime = 0;
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
        Ray.call(EchoTest::hi),
        Ray.call(EchoTest::who, who)
    ).get();
  }
}
