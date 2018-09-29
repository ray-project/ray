package org.ray.runtime.util.logger;

import org.ray.runtime.util.SystemUtil;

/**
 * loggers in Ray.
 *   help set the pid for worker log name
 */
public class RayLog {

  public static void init() {
    String loggingFileName = System.getProperty("ray.logging.file.name");
    if (loggingFileName != null && loggingFileName.contains("*pid_suffix*")) {
      loggingFileName = loggingFileName.replaceAll("\\*pid_suffix\\*",
          String.valueOf(SystemUtil.pid()));
      System.setProperty("ray.logging.file.name", loggingFileName);
    }
  }
}
