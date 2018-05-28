package org.ray.util.logger;

/**
 * Dynamic loggers in Ray
 */
public class RayLog {

  /**
   * for ray itself
   */
  public static final DynamicLog core = DynamicLog.registerName("core");

  /**
   * for ray's app's log
   */
  public static DynamicLog rapp = core; //DynamicLog.registerName("rapp");
}
