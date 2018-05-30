package org.ray.util.logger;

import org.apache.log4j.Logger;

/**
 * A logger which prints output to console
 */
public class ConsoleLogger extends Logger {

  final Logger realLogger;

  protected ConsoleLogger(String name, Logger realLogger) {
    super(name);
    this.realLogger = realLogger;
  }

  @Override
  public void info(Object log) {
    realLogger.info("(" + this.getName() + ") " + log);
  }

  @Override
  public void warn(Object log) {
    realLogger.warn("(" + this.getName() + ") " + log);
  }

  @Override
  public void warn(Object log, Throwable e) {
    realLogger.warn("(" + this.getName() + ") " + log, e);
  }

  @Override
  public void debug(Object log) {
    realLogger.debug("(" + this.getName() + ") " + log);
  }

  @Override
  public void error(Object log) {
    realLogger.error("(" + this.getName() + ") " + log);
  }

  @Override
  public void error(Object log, Throwable e) {
    realLogger.error("(" + this.getName() + ") " + log, e);
  }
}
