package org.ray.demo;

import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.annotation.RayRemote;

public class DemoApp {

  @RayRemote
  public static String sayHello() {
    String ret = "hello";
    return ret;
  }

  public static void main(String []args) {
    try {
      Ray.init();
      RayObject<String> hello = Ray.call(DemoApp::sayHello);
      System.out.println(hello.get());
    } catch (Throwable t) {
      t.printStackTrace();
    } finally {
      Ray.shutdown();
    }
  }

}
