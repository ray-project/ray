package org.ray.util.config;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A configuration section of related items.
 */
public class ConfigSection {

  public final Map<String, ConfigItem<?>> itemMap = new ConcurrentHashMap<>();
  public String sectionKey;
}
