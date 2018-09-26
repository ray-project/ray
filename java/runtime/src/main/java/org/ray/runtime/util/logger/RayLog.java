package org.ray.runtime.util.logger;

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

  public static void init() {
    core = LoggerFactory.getLogger("core");
    rapp = core;
  }
}
