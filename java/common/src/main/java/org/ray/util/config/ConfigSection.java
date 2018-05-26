package org.ray.util.config;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A configuration section of related items.
 */
public class ConfigSection {

  public String sectionKey;

  public final Map<String, ConfigItem<?>> itemMap = new ConcurrentHashMap<>();
}
