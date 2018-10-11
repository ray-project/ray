package org.ray.runtime.runner.worker;

import org.ray.api.Ray;

/**
 * The main function of DefaultDriver.
 */
public class DefaultDriver {

  /**
   * Command line:
   *
   * <p>java org.ray.runtime.runner.worker driverClassName [driverArg1, ...]
   *
   * <p>1. If you want to set `driver.id` to this driver, you should set it in system property,
   * like this:
   *       java -Dray.driver.id=0123456789ABCDEF0123
   *             org.ray.runtime.runner.worker.DefaultDriver driverClassName [driverArg1, ...]
   *
   * <p>2. DefaultDriver also needs `ray.conf` in classpath. You must specify
   *        driver jar package in classpath that DefaultDriver can load it to execute.
   */

  public static void main(String[] args) {

    if (args.length < 1) {
      throw new IllegalArgumentException(
          "Failed to start DefaultDriver: driverClassName was not specified.");
    }

    String driverClassName = args[0];
    final int driverArgsLength = args.length - 1;

    String[] driverArgs;
    if (driverArgsLength > 0) {
      driverArgs = new String[driverArgsLength];
      System.arraycopy(args, 1, driverArgs, 0, driverArgsLength);
    } else {
      driverArgs = new String[] {};
    }

    try {
      System.setProperty("ray.worker.mode", "DRIVER");
      Ray.init();
      Class<?> cls = Class.forName(driverClassName);
      cls.getMethod("main", String[].class).invoke(null, (Object) driverArgs);
    } catch (Throwable t) {
      t.printStackTrace();
    } finally {
      Ray.shutdown();
    }
  }
}
