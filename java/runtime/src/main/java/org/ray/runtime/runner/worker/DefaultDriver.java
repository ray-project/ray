package org.ray.runtime.runner.worker;

/**
 * The main function of DefaultDriver.
 */
public class DefaultDriver {

  /**
   * Command line:
   *
   * <p>java -ea org.ray.runtime.runner.worker driverClassName [driverArg1, ...]
   *
   * <p>1. If you want to set `driver.id` to this driver, you should set it in system property,
   * like this:
   *       java -ea -Dray.driver.id=0123456789ABCDEF0123
   *             org.ray.runtime.runner.worker driverClassName [driverArg1, ...]
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

    String[] dirverArgs;
    if (driverArgsLength > 0) {
      dirverArgs = new String[args.length - 1];
      System.arraycopy(args, 1, dirverArgs, 0, args.length - 1);
    } else {
      dirverArgs = new String [] {};
    }

    try {
      System.setProperty("ray.worker.mode", "DRIVER");

      Class<?> cls = Class.forName(driverClassName);
      cls.getMethod("main", String[].class).invoke(null, (Object) dirverArgs);
    } catch (Throwable e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }
}
