package org.ray.util.config;

/**
 * A ray configuration item of type {@code T}.
 */
public class ConfigItem<T> {

  public String key;

  public String oriValue;

  public T defaultValue;

  public String desc;
}
