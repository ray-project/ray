package demo;

import org.ray.api.annotation.RayRemote;

public class DemoApp {

  @RayRemote
  public static String hello() {
    return "hello";
  }
}
