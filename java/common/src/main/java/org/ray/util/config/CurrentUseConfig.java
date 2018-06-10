package org.ray.util.config;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * The configuration which is currently in use.
 */
public class CurrentUseConfig {

  public final Map<String, ConfigSection> sectionMap = new ConcurrentHashMap<>();
  public String filePath;

}
