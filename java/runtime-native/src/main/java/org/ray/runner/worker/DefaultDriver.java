package org.ray.runner.worker;

import org.ray.core.RayRuntime;
import org.ray.core.model.WorkerMode;

/**
 * The main function of DefaultDriver.
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
      String driverArgs = RayRuntime.configReader
          .getStringValue("ray.java.start", "driver_args", "",
              "arguments for the java class main function which is served at the driver");
      Class<?> cls = Class.forName(driverClass);
      String[] argsArray = (driverArgs != null) ? driverArgs.split(",") : (new String[] {});
      cls.getMethod("main", String[].class).invoke(null, (Object) argsArray);
    } catch (Throwable e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }
}
