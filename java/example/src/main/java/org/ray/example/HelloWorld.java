package org.ray.example;

import java.io.Serializable;
import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.RayRemote;
import org.ray.core.RayRuntime;
import org.ray.util.logger.RayLog;

public class HelloWorld implements Serializable {


  @RayRemote
  public static String sayHello() {
    String ret = "he";
    ret += "llo";
    RayLog.rapp.info("real say hello");
    return ret;
  }

  @RayRemote
  public static String sayWorld() {
    String ret = "world";
    ret += "!";
    return ret;
  }

  @RayRemote
  public static String merge(String hello, String world) {
    return hello + "," + world;
  }

  public static void main(String[] args) throws Exception {
    try {
      Ray.init();
      String helloWorld = HelloWorld.sayHelloWorld();
      RayLog.rapp.info(helloWorld);
      assert helloWorld.equals("hello,world!");
    } catch (Throwable t) {
      t.printStackTrace();
    } finally {
      RayRuntime.getInstance().cleanUp();
    }


  }

  public static String sayHelloWorld() {
    RayObject<String> hello = Ray.call(HelloWorld::sayHello);
    RayObject<String> world = Ray.call(HelloWorld::sayWorld);
    return Ray.call(HelloWorld::merge, hello, world).get();
  }
}
