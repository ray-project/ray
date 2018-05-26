package org.ray.runner.worker;

import org.ray.core.RayRuntime;
import org.ray.core.model.WorkerMode;

/**
 *
 */
public class DefaultDriver {

  //
  // " --node-ip-address=" + ip
  // + " --redis-address=" + redisAddress
  // + " --driver-class" + className
  //
  public static void main(String[] args) {
    try {
      RayRuntime.init(args);
      assert RayRuntime.getParams().worker_mode == WorkerMode.DRIVER;

      String driverClass = RayRuntime.configReader
          .getStringValue("ray.java.start", "driver_class", "",
              "java class which main is served as the driver in a java worker");
      Class<?> cls = Class.forName(driverClass);
      cls.getMethod("main", String[].class).invoke(null, (Object) new String[]{});
    } catch (Throwable e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }
}
