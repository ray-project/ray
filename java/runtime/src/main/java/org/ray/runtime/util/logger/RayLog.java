package org.ray.runtime.util.logger;

import org.ray.runtime.util.SystemUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * loggers in Ray.
 *   1. core logger is used for internal Ray status logging.
 *   2. rapp for ray applications logging.
 */
public class RayLog {

  /**
   * for ray itself.
   */
  public static Logger core;

  /**
   * for ray app.
   */
  public static Logger rapp;

  /**
   * Initialize loggers
   * @param logDir directory of the log files.
   */
  public static void init(String logDir) {
    String loggingPath = System.getProperty("logging.path");
    if (loggingPath == null) {
      System.setProperty("logging.path", logDir);
    }
    String loggingFileName = System.getProperty("logging.file.name");
    if (loggingFileName != null && loggingFileName.contains("*pid_suffix*")) {
      loggingFileName = loggingFileName.replaceAll("\\*pid_suffix\\*",
              String.valueOf(SystemUtil.pid()));
      System.setProperty("logging.file.name", loggingFileName);
    }

    core = LoggerFactory.getLogger("core");

    rapp = core;
  }
}
