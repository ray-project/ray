package org.ray.util.logger;

public enum LogLevel {
  ERROR("error", 0),
  WARN("warn", 1),
  INFO("info", 2),
  DEBUG("debug", 3);

  private final String name;
  private final int index;

  LogLevel(String name, int index) {
    this.name = name;
    this.index = index;
  }

  public static LogLevel of(String name) {
    for (LogLevel level : values()) {
      if (level.name.equals(name)) {
        return level;
      }
    }
    return null;
  }

  public Boolean needLog(LogLevel level) {
    return level.index <= this.index;
  }
}